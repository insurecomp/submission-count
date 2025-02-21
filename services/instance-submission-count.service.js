const fastify = require("../fastify");
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const {
  DynamoDBDocumentClient,
  ScanCommand,
  QueryCommand,
} = require("@aws-sdk/lib-dynamodb");

let moment = require("moment");
let numeral = require("numeral");
const client = new DynamoDBClient({ region: "us-east-1" }); // Set your region
const docClient = DynamoDBDocumentClient.from(client);
const zlib = require("zlib");

class instanceCountService {
  constructor() {}

  //Total Payroll Calculation
  icomppayrollCalculation = (childrenLoc) => {
    let totalPayroll = 0;
    for (const key in childrenLoc) {
      const classes = childrenLoc[key].classCodesInfo;
      for (const classKey in classes) {
        const payrollValue =
          classes[classKey]?.payroll?.value?.replace(/\$|,/g, "") || 0;
        totalPayroll += parseFloat(payrollValue);
      }
    }
    return totalPayroll;
  };

  //calculate Payroll
  getE3TotalPremium = (obj) => {
    let sum = 0;
    for (let key in obj) {
      sum += obj[key].total_standard_premium;
    }
    return sum;
  };

  // Fetch Pibit Company Name
  fetchPibitCompanyName = async (partitionKey) => {
    if (!partitionKey) {
      return 0;
    }
    const params = {
      TableName: "E3UserTable",
      KeyConditionExpression: "#id = :user_email_id",
      ExpressionAttributeNames: {
        "#id": "user_email_id",
      },
      ExpressionAttributeValues: {
        ":user_email_id": partitionKey,
      },
      ScanIndexForward: false,
      Limit: 1,
    };
    try {
      const data = await docClient.send(new QueryCommand(params)); // Use QueryCommand with the send() method
      if (data.Items[0]?.companyProfile) {
        let companyName = data.Items[0]?.companyProfile?.companyName?.value;
        return companyName;
      } else {
        return await getE3companyNameUserStatus(partitionKey);
      }
    } catch (error) {
      console.error("Error fetching Pibit company name:", error);
      throw new Error("Error fetching company name");
    }
  };

  // Fetch E3 HR Data from User Status Data
  fetchE3UserStatusData = async (partitionKey) => {
    if (!partitionKey) {
      return 0;
    }
    const params = {
      TableName: "E3UserStatusTable",
      KeyConditionExpression: "#id = :user_email_id",
      ExpressionAttributeNames: {
        "#id": "user_email_id",
      },
      ExpressionAttributeValues: {
        ":user_email_id": partitionKey,
      },
      ScanIndexForward: false,
      Limit: 1,
    };
    try {
      const data = await docClient.send(new QueryCommand(params)); // Use QueryCommand with send()
      if (data.Items[0]?.carrier_location_data) {
        let totalPremium = this.getE3TotalPremium(
          data.Items[0].carrier_location_data
        );
        return totalPremium;
      } else {
        return 0;
      }
    } catch (error) {
      console.error("Error fetching E3 user status data:", error);
      throw new Error("Error fetching user status data");
    }
  };

  //ICOMP Coloumn & Row setting
  icompCalculate = async (item) => {
    let obj = {};
    obj["CompanyName"] = item?.companyProfile?.companyName?.value || "";
    obj["FEIN"] = item?.companyProfile?.fein?.value || "";
    obj["Total Premium"] = await this.fetchE3UserStatusData(
      item?.user_email_id
    );
    let formstage = item?.formStage;
    let e3Status;
    if (formstage === "one" || formstage === "two") {
      e3Status = "In Progress";
    } else if (formstage === "three") {
      e3Status = "Pricing Requested";
    } else if (formstage === "four") {
      e3Status = "Pricing Aviable";
    } else if (formstage === "five") {
      e3Status = "View Proposal";
    }
    obj["Status"] = e3Status;
    obj["PEO"] = "E3 HR";
    obj["Total Payroll"] = item?.childrenLoc
      ? this.icomppayrollCalculation(item?.childrenLoc)
      : "$0";
    obj["Created Date"] = item?.uploadTimestamp
      ? moment(item?.uploadTimestamp, ["x"]).format("MM-DD-YYYY")
      : "";
    obj["LossRun"] = item?.workflowData?.data ? "YES" : "NO";
    console.log(obj);
    return obj;
  };

  //Table Scaning function for Libertate
  async getAllIcompData(instanceType) {
    try {
      let tableName = "E3UserTable";
      let params = { TableName: tableName };
      let finalResponse = [];
      let dbResponse;

      do {
        dbResponse = await docClient.send(new ScanCommand(params));
        for (let item of dbResponse.Items) {
          if (item?.companyProfile?.companyName?.value) {
            finalResponse.push(await this.icompCalculate(item));
          } else {
            finalResponse.push(await this.icompCalculate(item));
          }
        }
        params.ExclusiveStartKey = dbResponse.LastEvaluatedKey;
      } while (dbResponse.LastEvaluatedKey);

      return finalResponse;
    } catch (error) {
      console.error("Error scanning DynamoDB:", error);
      throw new Error(`Error scanning table for ${instanceType}`);
    }
  }

  async downloadIcompData(instanceType) {
    try {
      let finalResponse = await this.getAllIcompData(instanceType);
      return finalResponse;
    } catch (error) {
      console.error("Error in API:", error);
      return [];
    }
  }

  async getInstanceSubmissionCountData(type, request, reply) {
    try {
      if (type === "e3") {
        const response = await this.downloadIcompData(type);
        reply.send(response);
      }
    } catch (error) {
      console.error(`Error while Fetching ${request.params.type} data:`, error);
      reply.code(500).send(`Error while Fetching ${request.params.type} data.`);
    }
  }
}

module.exports = new instanceCountService();
