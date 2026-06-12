#!/usr/bin/env python3
"""
Morning-after rating-poll schedule manager for the Gracia bots — a LOCAL admin tool.

This is NOT part of the Lambda package (the deploy step zips only
lambda_function.py, and the deploy role only updates function CODE — it can't
create infra). Run this once, from a session with AWS admin creds, to create the
daily EventBridge rule that invokes the Lambda's morning-after poll — the same
way `tools/setup_webhooks.py` wires up Telegram.

What it creates (account 271378210266, region us-east-1):
  - an EventBridge (CloudWatch Events) rule `gracia-bots-morning-after`
    on `cron(0 6 * * ? *)`  ->  06:00 UTC daily  (~09:00 Europe/Kyiv in summer)
  - a target pointing at the `gracia-bots` Lambda with the constant input
    {"task": "morning_after"} — the marker the handler routes on
    (lambda_function._is_scheduled_event)
  - the `lambda:InvokeFunction` permission letting that rule invoke the function

The Lambda then scans the chat registry, finds recent un-rated winners, and posts
the 5★ rating poll via _post_rating_poll so votes feed get_ratings.

Note on time zone: classic EventBridge rules fire on a fixed UTC cron, so the
local time drifts ~1h across Kyiv DST. If you want it pinned to 09:00 local
year-round, create it in EventBridge Scheduler instead with
ScheduleExpressionTimezone="Europe/Kyiv" and cron(0 9 * * ? *) — see `console`.

Usage:
    python tools/setup_schedule.py set      # create/update the rule + target + permission
    python tools/setup_schedule.py info     # show the rule, its targets, and next-ish state
    python tools/setup_schedule.py test     # invoke the Lambda once with the cron payload now
    python tools/setup_schedule.py delete   # remove the target + rule (leaves the Lambda)
    python tools/setup_schedule.py console  # print exact AWS Console / CLI steps to do it by hand
"""
import json
import sys

REGION = "us-east-1"
ACCOUNT = "271378210266"
FUNCTION = "gracia-bots"
RULE = "gracia-bots-morning-after"
CRON = "cron(0 6 * * ? *)"                       # 06:00 UTC daily (~09:00 Kyiv, summer)
INPUT = {"task": "morning_after"}                # matches _is_scheduled_event in the Lambda
STATEMENT_ID = "gracia-bots-morning-after-invoke"
TARGET_ID = "morning-after"


def _clients():
    import boto3
    return boto3.client("events", region_name=REGION), boto3.client("lambda", region_name=REGION)


def cmd_set():
    events, lam = _clients()
    func_arn = lam.get_function(FunctionName=FUNCTION)["Configuration"]["FunctionArn"]
    rule_arn = events.put_rule(Name=RULE, ScheduleExpression=CRON, State="ENABLED",
                               Description="Daily morning-after rating poll for movie winners"
                               )["RuleArn"]
    print(f"rule:   {rule_arn}  [{CRON}]")
    # Allow this rule to invoke the function (idempotent: ignore if it already exists).
    try:
        lam.add_permission(FunctionName=FUNCTION, StatementId=STATEMENT_ID,
                           Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                           SourceArn=rule_arn)
        print(f"perm:   added {STATEMENT_ID}")
    except lam.exceptions.ResourceConflictException:
        print(f"perm:   {STATEMENT_ID} already present")
    events.put_targets(Rule=RULE, Targets=[{
        "Id": TARGET_ID, "Arn": func_arn, "Input": json.dumps(INPUT)}])
    print(f"target: {FUNCTION}  input={json.dumps(INPUT)}")
    print("done — the morning-after poll will run daily.")


def cmd_info():
    events, _ = _clients()
    try:
        r = events.describe_rule(Name=RULE)
    except events.exceptions.ResourceNotFoundException:
        print(f"rule {RULE!r} does not exist — run `set`.")
        return
    print(f"{r['Name']}: {r.get('ScheduleExpression')}  state={r.get('State')}")
    for t in events.list_targets_by_rule(Rule=RULE).get("Targets", []):
        print(f"  -> {t['Arn']}  input={t.get('Input')}")


def cmd_test():
    _, lam = _clients()
    resp = lam.invoke(FunctionName=FUNCTION, InvocationType="RequestResponse",
                      Payload=json.dumps(INPUT).encode())
    print("status:", resp["StatusCode"])
    print("body:  ", resp["Payload"].read().decode())


def cmd_delete():
    events, lam = _clients()
    try:
        events.remove_targets(Rule=RULE, Ids=[TARGET_ID])
        events.delete_rule(Name=RULE)
        print(f"removed rule {RULE} + target")
    except events.exceptions.ResourceNotFoundException:
        print(f"rule {RULE!r} not found")
    try:
        lam.remove_permission(FunctionName=FUNCTION, StatementId=STATEMENT_ID)
        print(f"removed permission {STATEMENT_ID}")
    except lam.exceptions.ResourceNotFoundException:
        pass


def cmd_console():
    input_json = json.dumps(INPUT)
    input_escaped = input_json.replace('"', '\\"')      # for the Scheduler --target string
    print(f"""Create the daily morning-after schedule by hand (mirrors the webhook setup):

AWS Console -> Amazon EventBridge -> Rules -> Create rule
  Name:            {RULE}
  Event bus:       default
  Rule type:       Schedule
  Schedule:        Cron expression  ->  {CRON}   (06:00 UTC daily, ~09:00 Kyiv)
  Target:          AWS Lambda function -> {FUNCTION}
  Additional settings -> Configure target input -> Constant (JSON text):
                   {input_json}
  Save. (The console adds the lambda:InvokeFunction permission for you.)

Equivalent CLI:
  FN_ARN=$(aws lambda get-function --function-name {FUNCTION} \\
    --query Configuration.FunctionArn --output text --region {REGION})
  aws events put-rule --name {RULE} --schedule-expression "{CRON}" \\
    --state ENABLED --region {REGION}
  aws lambda add-permission --function-name {FUNCTION} \\
    --statement-id {STATEMENT_ID} --action lambda:InvokeFunction \\
    --principal events.amazonaws.com \\
    --source-arn arn:aws:events:{REGION}:{ACCOUNT}:rule/{RULE} --region {REGION}
  aws events put-targets --rule {RULE} --region {REGION} \\
    --targets "Id"="{TARGET_ID}","Arn"="$FN_ARN","Input"='{input_json}'

For exact 09:00 Kyiv year-round, use EventBridge Scheduler instead:
  aws scheduler create-schedule --name {RULE} \\
    --schedule-expression "cron(0 9 * * ? *)" \\
    --schedule-expression-timezone "Europe/Kyiv" \\
    --flexible-time-window '{{"Mode":"OFF"}}' \\
    --target '{{"Arn":"<FN_ARN>","RoleArn":"<scheduler-invoke-role-arn>","Input":"{input_escaped}"}}' \\
    --region {REGION}
""")


_COMMANDS = {"set": cmd_set, "info": cmd_info, "test": cmd_test,
             "delete": cmd_delete, "console": cmd_console}

if __name__ == "__main__":
    action = sys.argv[1] if len(sys.argv) > 1 else ""
    if action not in _COMMANDS:
        print(f"usage: python tools/setup_schedule.py {{{'|'.join(_COMMANDS)}}}")
        sys.exit(2)
    _COMMANDS[action]()
