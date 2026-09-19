#!/bin/bash
# Runs inside LocalStack when it is ready. Provisions the FIFO queues used by
# the service: the inbound wager queue with its DLQ (redrive after 5
# receives) and the outbound integration events queue with its own DLQ.
set -euo pipefail

REGION="${AWS_DEFAULT_REGION:-us-east-1}"
MAX_RECEIVE="${WAGER_MAX_RECEIVE_COUNT:-5}"
VISIBILITY="${WAGER_VISIBILITY_TIMEOUT:-30}"

create_fifo() {
  local name="$1"; shift
  awslocal sqs create-queue --queue-name "$name" --attributes "$@" >/dev/null
  echo "queue $name ready"
}

arn_of() {
  awslocal sqs get-queue-attributes --queue-url "$(awslocal sqs get-queue-url --queue-name "$1" --query QueueUrl --output text)" \
    --attribute-names QueueArn --query Attributes.QueueArn --output text
}

create_fifo wager-transactions-dlq.fifo '{"FifoQueue":"true","ContentBasedDeduplication":"false","MessageRetentionPeriod":"1209600"}'
DLQ_ARN="$(arn_of wager-transactions-dlq.fifo)"
# Queue policy: providers may only send; the wager service may only consume.
# LocalStack (community) stores but does not enforce IAM policies; on AWS the
# same document restricts access by principal.
ACCOUNT=000000000000
POLICY=$(cat <<JSON
{"Version":"2012-10-17","Statement":[
 {"Sid":"ProvidersPublish","Effect":"Allow","Principal":{"AWS":["arn:aws:iam::${ACCOUNT}:role/provider-a","arn:aws:iam::${ACCOUNT}:role/provider-b"]},"Action":["sqs:SendMessage"],"Resource":"arn:aws:sqs:${REGION}:${ACCOUNT}:wager-transactions.fifo"},
 {"Sid":"ServiceConsumes","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::${ACCOUNT}:role/wager-service"},"Action":["sqs:ReceiveMessage","sqs:DeleteMessage","sqs:ChangeMessageVisibility","sqs:GetQueueAttributes","sqs:GetQueueUrl"],"Resource":"arn:aws:sqs:${REGION}:${ACCOUNT}:wager-transactions.fifo"}
]}
JSON
)
POLICY_ESCAPED=$(printf '%s' "$POLICY" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))' 2>/dev/null || printf '"%s"' "$(printf '%s' "$POLICY" | sed 's/"/\\"/g')")
create_fifo wager-transactions.fifo "{\"FifoQueue\":\"true\",\"ContentBasedDeduplication\":\"false\",\"VisibilityTimeout\":\"${VISIBILITY}\",\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"${DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"${MAX_RECEIVE}\\\"}\",\"Policy\":${POLICY_ESCAPED}}"

create_fifo wallet-events-dlq.fifo '{"FifoQueue":"true","ContentBasedDeduplication":"false","MessageRetentionPeriod":"1209600"}'
EVENTS_DLQ_ARN="$(arn_of wallet-events-dlq.fifo)"
create_fifo wallet-events.fifo "{\"FifoQueue\":\"true\",\"ContentBasedDeduplication\":\"false\",\"VisibilityTimeout\":\"30\",\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"${EVENTS_DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"5\\\"}\"}"

awslocal sqs list-queues
