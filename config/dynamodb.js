const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient } = require("@aws-sdk/lib-dynamodb");
const fp = require("fastify-plugin");

async function dynamoDBConnector(fastify, options) {
  try {
    // Directly access environment variables from serverless.yml
    const REGION = process.env.AWS_REGION;

    // Initialize DynamoDB Client
    const dynamoClient = new DynamoDBClient({
      region: REGION,
    });

    // Simplified DynamoDB Document Client for JSON handling
    const docClient = DynamoDBDocumentClient.from(dynamoClient);
    console.log("DynamoDB Connected");

    // Attach DynamoDB client to Fastify
    fastify.decorate("dynamo", docClient);

    // Clean up connection on shutdown
    fastify.addHook("onClose", async (instance, done) => {
      console.log("DynamoDB connection closed");
      done();
    });
  } catch (err) {
    console.error("DynamoDB connection error:", err);
    process.exit(1);
  }
}

module.exports = fp(dynamoDBConnector);
