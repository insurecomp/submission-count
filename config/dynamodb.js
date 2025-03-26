const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient } = require("@aws-sdk/lib-dynamodb");
const fp = require("fastify-plugin");
const { NodeHttpHandler } = require("@smithy/node-http-handler");
const https = require("https");
const { COMMON } = require("./config");

/** Enable connection reuse */
process.env.AWS_NODEJS_CONNECTION_REUSE_ENABLED = "1";

async function dynamoDBConnector(fastify, options) {
  try {
    const agent = new https.Agent({
      keepAlive: true,
      maxSockets: 100 /** Allow 100 parallel connections */,
    });

    const httpHandler = new NodeHttpHandler({
      httpAgent: agent,
      connectionTimeout: 30000 /** Set connection timeout (optional) */,
      socketTimeout: 60000 /** Set socket timeout (optional) */,
      maxSockets: 200 /** Increase max sockets from 50 to 200 */,
    });

    const dynamoClient = new DynamoDBClient({
      region: COMMON.AWS_REGION,
      requestHandler: httpHandler,
    });

    const docClient = DynamoDBDocumentClient.from(dynamoClient);
    console.log("DynamoDB Connected");

    fastify.decorate("dynamo", docClient);

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
