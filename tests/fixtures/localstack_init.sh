#!/bin/bash
# Creates DynamoDB tables with streams enabled for CDC testing.
set -e

awslocal dynamodb create-table \
  --table-name customers \
  --attribute-definitions AttributeName=id,AttributeType=S \
  --key-schema AttributeName=id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES

awslocal dynamodb create-table \
  --table-name orders \
  --attribute-definitions AttributeName=id,AttributeType=S \
  --key-schema AttributeName=id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES

awslocal dynamodb put-item --table-name customers --item '{"id":{"S":"1"},"name":{"S":"Alice"},"email":{"S":"alice@example.com"}}'
awslocal dynamodb put-item --table-name customers --item '{"id":{"S":"2"},"name":{"S":"Bob"},"email":{"S":"bob@example.com"}}'
awslocal dynamodb put-item --table-name customers --item '{"id":{"S":"3"},"name":{"S":"Charlie"},"email":{"S":"charlie@example.com"}}'

awslocal dynamodb put-item --table-name orders --item '{"id":{"S":"1"},"customer_id":{"S":"1"},"amount":{"N":"99.99"},"status":{"S":"completed"}}'
awslocal dynamodb put-item --table-name orders --item '{"id":{"S":"2"},"customer_id":{"S":"2"},"amount":{"N":"149.50"},"status":{"S":"pending"}}'
awslocal dynamodb put-item --table-name orders --item '{"id":{"S":"3"},"customer_id":{"S":"1"},"amount":{"N":"25.00"},"status":{"S":"completed"}}'
