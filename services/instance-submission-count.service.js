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

class instanceCountService {
  constructor() {}

  //Total Payroll Calculation for E3,EXTENSIS
  payrollCalculation = (childrenLoc) => {
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

  getHighestPayrollDetails(childrenLoc) {
    let maxPayroll = -Infinity;
    let maxClassCode = null;
    let maxState = null;

    for (let key in childrenLoc) {
      let childrenLocObject = childrenLoc[key];
      let classCodes = childrenLocObject.classCodesInfo;
      let state = childrenLocObject.state.value;

      for (let ele in classCodes) {
        let payroll = Number(
          classCodes[ele].payroll.value.replace(/[\$,]/g, "")
        );
        let classCode =
          classCodes[ele].classCodeDescription.value.split(":")[0]; // Extract class code

        if (payroll > maxPayroll) {
          maxPayroll = payroll;
          maxClassCode = classCode;
          maxState = state;
        }
      }
    }

    return [maxClassCode, maxState];
  }

  //------------E3 Start Here----------------------//

  //calculate Payroll
  getE3TotalPremium = (obj) => {
    let sum = 0;
    for (let key in obj) {
      sum += obj[key].total_standard_premium;
    }
    return sum;
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

  //get Pibit OCR Date
  getPibitOCRdate = async (userID) => {
    try {
      const params = {
        TableName: "E3PibitOcrTrigger",
        IndexName: "userID-index",
        KeyConditionExpression: "userID = :userID",
        ExpressionAttributeValues: {
          ":userID": userID,
        },
      };
      const command = new QueryCommand(params);
      const response = await docClient.send(command);
      let date = response.Items[0]?.createdTimestamp;
      let e3CreatedDate = date
        ? moment(date + "000", ["x"]).format("MM-DD-YYYY")
        : "";
      return e3CreatedDate;
    } catch (error) {
      console.error("Error fetching items:", error);
      return [];
    }
  };

  //E3 Coloumn & Row setting
  e3RowCalculate = async (item) => {
    let obj = {};
    obj["Unique Id"] = item?.user_email_id;
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
      e3Status = "Pricing Available";
    } else if (formstage === "five") {
      e3Status = "View Proposal";
    }
    obj["Status"] = e3Status;
    obj["PEO"] = "E3 HR";
    obj["Total Payroll"] = item?.childrenLoc
      ? this.payrollCalculation(item?.childrenLoc)
      : 0;
    obj["Created Date"] = item?.uploadTimestamp
      ? moment(item?.uploadTimestamp, ["x"]).format("MM-DD-YYYY")
      : "";
    obj["LossRun"] = item?.workflowData?.data ? "YES" : "NO";
    obj["LossRun Date"] = await this.getPibitOCRdate(item?.user_email_id);
    obj["GoverningState"] =
      this.getHighestPayrollDetails(item?.childrenLoc)?.[1] || "Null";
    obj["GoverningCC"] =
      this.getHighestPayrollDetails(item?.childrenLoc)?.[0] || "Null";
    obj["Description"] =
      item?.companyProfile?.descriptionOfOperations?.value || "null";
    obj["ExpiryDate"] =
      item?.companyProfile?.expectedExpiryDate?.value || "null";
    obj["EffectiveDate"] = item?.companyProfile?.effectiveDate?.value || "null";
    obj["AgentName"] = item?.modifiedBy
      ? item?.modifiedBy.split("@")[0]
      : "Null";
    console.log(obj);
    return obj;
  };

  //Table Scaning function for E3
  async getAllE3Data(instanceType) {
    try {
      let tableName = "E3UserTable";
      let params = { TableName: tableName };
      let finalResponse = [];
      let dbResponse;

      do {
        dbResponse = await docClient.send(new ScanCommand(params));
        for (let item of dbResponse.Items) {
          if (item?.companyProfile?.companyName?.value) {
            finalResponse.push(await this.e3RowCalculate(item));
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

  //Feching E3 Data
  async downloadE3Data(instanceType) {
    try {
      let finalResponse = await this.getAllE3Data(instanceType);
      return finalResponse;
    } catch (error) {
      console.error("Error in API:", error);
      return [];
    }
  }

  //------------E3 END Here----------------------//

  //-------------Extensis Start Here--------------------//
  extensisCalculate = async (item) => {
    let obj = {};
    obj["Opportunity ID"] = item?.opportunity_id || "";
    obj["Effective Date"] = item?.effective_date || "";
    obj["CompanyName"] = item?.companyProfile?.companyName?.value || "";
    obj["FEIN"] = item?.companyProfile?.fein?.value || "";
    let status = item?.status;
    if (item.existing === "true") {
      status = "Bound";
    } else if (status === "sent_to_salesforce") {
      status = "To Salesforce";
    } else if (status === "quote_generated") {
      status = "In Progress";
    } else {
      status = "Submitted";
    }
    obj["Status"] = status;
    obj["Total Payroll"] = item?.payrollData
      ? this.payrollCalculation(item?.payrollData)
      : "$0";
    obj["Created Date"] = item?.uploadTimestamp
      ? moment(item?.uploadTimestamp, ["x"]).format("MM-DD-YYYY")
      : "";
    obj["Agent Name"] = item?.modifiedByName || "";
    obj["Agenct Email"] = item?.modifiedBy || "";
    obj["Type"] = "New Business";
    console.log(obj);
    return obj;
  };

  async downloadExtensisData() {
    let finalResponse = [];
    let params = {
      TableName: "ExtensisOpportunityData",
    };

    try {
      let data;
      do {
        data = await docClient.send(new ScanCommand(params));
        for (let item of data.Items) {
          finalResponse.push(await this.extensisCalculate(item));
        }
        params.ExclusiveStartKey = data.LastEvaluatedKey;
      } while (typeof data.LastEvaluatedKey !== "undefined");
      return finalResponse;
    } catch (error) {
      console.error("Error fetching items from DynamoDB:", error);
      return [];
    }
  }
  //-------------Extensis END Here--------------------//

  //-------------IES Start Here---------------------//
  fetchIESpibitOCR = async (userID) => {
    try {
      const params = {
        TableName: "PibitOcrTriggerProd",
        IndexName: "userID-index",
        KeyConditionExpression: "userID = :userID",
        ExpressionAttributeValues: {
          ":userID": userID,
        },
      };
      const command = new QueryCommand(params);
      const response = await docClient.send(command);
      let date = response.Items[0]?.createdTimestamp;
      let e3CreatedDate = date
        ? moment(date + "000", ["x"]).format("MM-DD-YYYY")
        : "";
      return e3CreatedDate;
    } catch (error) {
      console.error("Error fetching items:", error);
      return [];
    }
  };

  iesorLibCalculate = async (item, instanceType) => {
    let obj = {};
    obj["CompanyName"] = item?.companyProfile?.companyName?.value || "";
    obj["FEIN"] = item?.companyProfile?.fein?.value || "";
    let status;
    let dbStatus = item?.status || "";
    if (dbStatus === "quote_generated" || dbStatus === "view_proposal") {
      status = "Quote Generated";
    } else if (dbStatus === "company_profile") {
      status = "Underwriting Page";
    } else {
      status = "API";
    }
    obj["Status"] = status;
    obj["PEO"] = item?.peoDetails?.selectedPeo || "";
    obj["Total Payroll"] = item?.childrenLoc
      ? this.payrollCalculation(item?.childrenLoc)
      : "$0";

    obj["Created Date"] = item?.uploadTimestamp
      ? moment(item?.uploadTimestamp, ["x"]).format("MM-DD-YYYY")
      : "";

    if (instanceType === "ies") {
      let lossRunData = await this.fetchIESpibitOCR(item?.user_email_id);
      obj["LossRun"] = lossRunData ? "YES" : "NO";
      obj["lossRunUploadDate"] = lossRunData ? lossRunData : "NULL";
    }
    console.log(obj);
    return obj;
  };

  async downloadIESorLibertateData(instanceType) {
    let finalResponse;
    let libResponse = [];
    let iesResponse = [];
    let params = {
      TableName: "Icomp2UserTable",
    };

    try {
      let data;
      do {
        data = await docClient.send(new ScanCommand(params));
        for (let item of data.Items) {
          if (
            instanceType === "ies" &&
            (item.origin_instance === "ies" || item.salesforceData)
          ) {
            iesResponse.push(await this.iesorLibCalculate(item, instanceType));
          } else {
            libResponse.push(await this.iesorLibCalculate(item, instanceType));
          }
        }
        params.ExclusiveStartKey = data.LastEvaluatedKey;
      } while (typeof data.LastEvaluatedKey !== "undefined");

      if (instanceType === "ies") {
        finalResponse = iesResponse;
      } else {
        finalResponse = libResponse;
      }

      return finalResponse;
    } catch (error) {
      console.error("Error fetching items from DynamoDB:", error);
      reply.code(500).send("Error fetching items from DynamoDB:", error);
    }
  }
  //-------------IES END Here---------------------//

  async getInstanceSubmissionCountData(type, request, reply) {
    try {
      if (type === "e3") {
        const response = await this.downloadE3Data(type);
        reply.send(response);
      } else if (type === "extensis") {
        const response = await this.downloadExtensisData();
        reply.send(response);
      } else if (type === "ies") {
        const response = await this.downloadIESorLibertateData(type);
        reply.send(response);
      }
    } catch (error) {
      console.error(`Error while Fetching ${request.params.type} data:`, error);
      return [];
    }
  }
}

module.exports = new instanceCountService();
