#!/usr/bin/env python3
"""
EventBridge schedule manager for the Gracia bots — a LOCAL admin tool.

This is NOT part of the Lambda package (the deploy step zips only
lambda_function.py, and the deploy role only updates function CODE — it can't
create infra). Run this once, from a session with AWS admin creds, to create the
scheduled rules that invoke the Lambda's background jobs — the same way
`tools/setup_webhooks.py` wires up Telegram.

Two jobs (account 271378210266, region us-east-1), each an EventBridge rule whose
target is the `gracia-bots` Lambda with a constant input carrying the top-level
`task` marker the handler routes on (lambda_function._scheduled_task):

  morning-after   rule gracia-bots-morning-after   cron(0 6 * * ? *)  (06:00 UTC daily,
                  input {"task":"morning_after"}    ~09:00 Kyiv in summer)
                  -> posts the 5★ rating poll for recent un-rated movie winners.

  constraint-tick rule gracia-bots-constraint-tick  rate(1 minute)
                  input {"task":"constraint_tick"}
                  -> closes any constraint-collection window whose 60s deadline has
                     passed, so it fires even when the chat goes silent. (The window
                     is never closed early; the sweep only acts after the deadline.)

Each rule also gets the `lambda:InvokeFunction` permission letting it invoke the
function.

Note on time zone: classic EventBridge rules fire on a fixed UTC cron, so the
morning-after local time drifts ~1h across Kyiv DST. To pin it to 09:00 local
year-round, create it in EventBridge Scheduler instead with
ScheduleExpressionTimezone="Europe/Kyiv" and cron(0 9 * * ? *) — see `console`.

Usage:
    python tools/setup_schedule.py set      # create/update every rule + target + permission
    python tools/setup_schedule.py info     # show each rule and its target
    python tools/setup_schedule.py test     # invoke the Lambda once per job with its payload
    python tools/setup_schedule.py delete   # remove the targets + rules (leaves the Lambda)
    python tools/setup_schedule.py console  # print exact AWS Console / CLI steps to do it by hand
"""
import json
import sys

REGION = "us-east-1"
ACCOUNT = "271378210266"
FUNCTION = "gracia-bots"

# Each job is one EventBridge rule -> the Lambda, carrying its `task` marker as input.
JOBS = [
    {"rule": "gracia-bots-morning-after", "expr": "cron(0 6 * * ? *)",
     "input": {"task": "morning_after"}, "target_id": "morning-after",
     "stmt": "gracia-bots-morning-after-invoke",
     "desc": "Daily morning-after rating poll for movie winners (06:00 UTC ~ 09:00 Kyiv)"},
    {"rule": "gracia-bots-constraint-tick", "expr": "rate(1 minute)",
     "input": {"task": "constraint_tick"}, "target_id": "constraint-tick",
     "stmt": "gracia-bots-constraint-tick-invoke",
     "desc": "Close timed constraint-collection windows (fires even when the chat is silent)"},
]


def _clients():
    import boto3
    return boto3.client("events", region_name=REGION), boto3.client("lambda", region_name=REGION)


def cmd_set():
    events, lam = _clients()
    func_arn = lam.get_function(FunctionName=FUNCTION)["Configuration"]["FunctionArn"]
    for job in JOBS:
        rule_arn = events.put_rule(Name=job["rule"], ScheduleExpression=job["expr"],
                                   State="ENABLED", Description=job["desc"])["RuleArn"]
        print(f"rule:   {rule_arn}  [{job['expr']}]")
        # Allow this rule to invoke the function (idempotent: ignore if already present).
        try:
            lam.add_permission(FunctionName=FUNCTION, StatementId=job["stmt"],
                               Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                               SourceArn=rule_arn)
            print(f"perm:   added {job['stmt']}")
        except lam.exceptions.ResourceConflictException:
            print(f"perm:   {job['stmt']} already present")
        events.put_targets(Rule=job["rule"], Targets=[{
            "Id": job["target_id"], "Arn": func_arn, "Input": json.dumps(job["input"])}])
        print(f"target: {FUNCTION}  input={json.dumps(job['input'])}")
    print("done — scheduled jobs are live.")


def cmd_info():
    events, _ = _clients()
    for job in JOBS:
        try:
            r = events.describe_rule(Name=job["rule"])
        except events.exceptions.ResourceNotFoundException:
            print(f"{job['rule']}: (does not exist — run `set`)")
            continue
        print(f"{r['Name']}: {r.get('ScheduleExpression')}  state={r.get('State')}")
        for t in events.list_targets_by_rule(Rule=job["rule"]).get("Targets", []):
            print(f"  -> {t['Arn']}  input={t.get('Input')}")


def cmd_test():
    _, lam = _clients()
    for job in JOBS:
        resp = lam.invoke(FunctionName=FUNCTION, InvocationType="RequestResponse",
                          Payload=json.dumps(job["input"]).encode())
        body = resp["Payload"].read().decode()
        print(f"{job['target_id']}: status={resp['StatusCode']}  body={body}")


def cmd_delete():
    events, lam = _clients()
    for job in JOBS:
        try:
            events.remove_targets(Rule=job["rule"], Ids=[job["target_id"]])
            events.delete_rule(Name=job["rule"])
            print(f"removed rule {job['rule']} + target")
        except events.exceptions.ResourceNotFoundException:
            print(f"rule {job['rule']!r} not found")
        try:
            lam.remove_permission(FunctionName=FUNCTION, StatementId=job["stmt"])
            print(f"removed permission {job['stmt']}")
        except lam.exceptions.ResourceNotFoundException:
            pass


def cmd_console():
    for job in JOBS:
        input_json = json.dumps(job["input"])
        print(f"""--- {job['rule']} -----------------------------------------------------
AWS Console -> Amazon EventBridge -> Rules -> Create rule
  Name:            {job['rule']}
  Event bus:       default
  Rule type:       Schedule
  Schedule:        {job['expr']}
  Target:          AWS Lambda function -> {FUNCTION}
  Additional settings -> Configure target input -> Constant (JSON text):
                   {input_json}
  Save. (The console adds the lambda:InvokeFunction permission for you.)

Equivalent CLI:
  FN_ARN=$(aws lambda get-function --function-name {FUNCTION} \\
    --query Configuration.FunctionArn --output text --region {REGION})
  aws events put-rule --name {job['rule']} --schedule-expression "{job['expr']}" \\
    --state ENABLED --region {REGION}
  aws lambda add-permission --function-name {FUNCTION} \\
    --statement-id {job['stmt']} --action lambda:InvokeFunction \\
    --principal events.amazonaws.com \\
    --source-arn arn:aws:events:{REGION}:{ACCOUNT}:rule/{job['rule']} --region {REGION}
  aws events put-targets --rule {job['rule']} --region {REGION} \\
    --targets "Id"="{job['target_id']}","Arn"="$FN_ARN","Input"='{input_json}'
""")
    ma_input = json.dumps(JOBS[0]["input"]).replace('"', '\\"')
    print(f"""For exact 09:00 Kyiv year-round, create morning-after in EventBridge Scheduler:
  aws scheduler create-schedule --name {JOBS[0]['rule']} \\
    --schedule-expression "cron(0 9 * * ? *)" \\
    --schedule-expression-timezone "Europe/Kyiv" \\
    --flexible-time-window '{{"Mode":"OFF"}}' \\
    --target '{{"Arn":"<FN_ARN>","RoleArn":"<scheduler-invoke-role-arn>","Input":"{ma_input}"}}' \\
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
