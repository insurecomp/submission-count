const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const {
  DynamoDBDocumentClient,
  ScanCommand,
  QueryCommand,
} = require("@aws-sdk/lib-dynamodb");
const moment = require("moment");
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

    for (const key in childrenLoc) {
      const childrenLocObject = childrenLoc[key];
      const classCodes = childrenLocObject.classCodesInfo;
      const state = childrenLocObject.state.value;

      for (const ele in classCodes) {
        const payroll = Number(
          classCodes[ele].payroll.value.replace(/[\$,]/g, "")
        );
        const classCode =
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
    for (const key in obj) {
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
        const totalPremium = this.getE3TotalPremium(
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

  fetchE3CarrierSelected = async (partitionKey) => {
    if (!partitionKey) {
      return 0;
    }
    const carrierMap = {
      carrier_az: "Arch",
      carrier_ba: "SUNZ",
      carrier_bc: "Prescient",
    };
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
      const data = await docClient.send(new QueryCommand(params));
      if (data.Items[0]?.tableCarrierSelect) {
        const carrier = carrierMap[data.Items[0]?.tableCarrierSelect]
          ? carrierMap[data.Items[0]?.tableCarrierSelect]
          : data.Items[0]?.tableCarrierSelect;
        return carrier;
      } else {
        return "NULL";
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
      const date = response.Items[0]?.createdTimestamp;
      const e3CreatedDate = date
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
    const obj = {};
    obj["Unique Id"] = item?.user_email_id;
    obj["CompanyName"] = item?.companyProfile?.companyName?.value || "";
    obj["FEIN"] = item?.companyProfile?.fein?.value || "";
    obj["Total Premium"] = await this.fetchE3UserStatusData(
      item?.user_email_id
    );
    const formstage = item?.formStage;
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
    obj["QuoteDate"] = item?.quoteData?.date
      ? moment(item?.quoteData?.date, ["x"]).format("MM-DD-YYYY")
      : "NULL";
    obj["SelectedCarrier"] = await this.fetchE3CarrierSelected(
      item?.user_email_id
    );
    console.log(obj);
    return obj;
  };

  //Table Scaning function for E3
  async getAllE3Data(instanceType) {
    try {
      const tableName = "E3UserTable";
      const params = { TableName: tableName };
      const finalResponse = [];
      let dbResponse;

      do {
        dbResponse = await docClient.send(new ScanCommand(params));
        for (const item of dbResponse.Items) {
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
      const finalResponse = await this.getAllE3Data(instanceType);
      return finalResponse;
    } catch (error) {
      console.error("Error in API:", error);
      return [];
    }
  }

  //------------E3 END Here----------------------//

  //-------------Extensis Start Here--------------------//
  extensisCalculate = async (item) => {
    const obj = {};
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
    const finalResponse = [];
    const params = {
      TableName: "ExtensisOpportunityData",
    };

    try {
      let data;
      do {
        data = await docClient.send(new ScanCommand(params));
        for (const item of data.Items) {
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
      const date = response.Items[0]?.createdTimestamp;
      const e3CreatedDate = date
        ? moment(date + "000", ["x"]).format("MM-DD-YYYY")
        : "";
      return e3CreatedDate;
    } catch (error) {
      console.error("Error fetching items:", error);
      return [];
    }
  };

  libCalculate = async (item, instanceType) => {
    const obj = {};
    obj["CompanyName"] = item?.companyProfile?.companyName?.value || "";
    obj["FEIN"] = item?.companyProfile?.fein?.value || "";
    let status;
    const dbStatus = item?.status || "";
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

    obj["Created Date"] = item?.createdDate
      ? moment(item?.createdDate, ["x"]).format("MM-DD-YYYY")
      : "";
    console.log(obj);
    return obj;
  };

  async downloadIESData(instanceType) {
    const iesResponse = [];
    const params = {
      TableName: "Icomp2UserTable",
      FilterExpression: "#origin = :ies OR attribute_exists(salesforceData)",
      ExpressionAttributeNames: {
        "#origin": "origin_instance",
      },
      ExpressionAttributeValues: {
        ":ies": "ies",
      },
    };

    try {
      let data;
      do {
        data = await docClient.send(new ScanCommand(params));
        for (const item of data.Items) {
          iesResponse.push(await this.libCalculate(item, instanceType));
        }
        params.ExclusiveStartKey = data.LastEvaluatedKey;
      } while (typeof data.LastEvaluatedKey !== "undefined");
      return iesResponse;
    } catch (error) {
      console.error("Error fetching items from DynamoDB:", error);
    }
  }
  //-------------IES END Here---------------------//

  //-------------RTIA START HERE------------------//
  getGoverningClassCode = (childerLoc) => {
    const arr1 = [];
    const arr2 = [];
    for (const key in childerLoc) {
      const obj = childerLoc[key];
      const classCodesInfoObj = obj?.classCodesInfo;
      for (const indx in classCodesInfoObj) {
        const classCode =
          classCodesInfoObj[indx]?.classCodeDescription?.value?.split(":")[0];
        const payroll = classCodesInfoObj[indx]?.payroll?.value;
        const number = parseInt(payroll?.replace(/[\$,]/g, ""), 10);
        arr1.push(classCode);
        arr2.push(number);
      }
    }
    const map = new Map();
    for (let i = 0; i < arr2.length; i++) {
      map.set(i, arr2[i]);
    }
    const max = Math.max(...arr2);
    let arr1_index;
    for (const [k, v] of map) {
      if (v === max) {
        arr1_index = k;
      }
    }

    return arr1[arr1_index];
  };

  getGoverningState = (childerLoc) => {
    const payrollByState = {};
    let highestPayrollState = null;
    let highestPayroll = 0;
    for (const key in childerLoc) {
      const state = childerLoc[key].state.value;
      const classCodesInfo = childerLoc[key].classCodesInfo;
      // Initialize state's payroll if not already
      if (!payrollByState[state]) {
        payrollByState[state] = 0;
      }
      // Sum up all payrolls for this state
      for (const classKey in classCodesInfo) {
        const payrollValue = classCodesInfo[classKey]?.payroll?.value;
        const payrollAmount = parseInt(payrollValue?.replace(/[\$,]/g, ""), 10);
        payrollByState[state] += payrollAmount;
      }
      // Check if this state has the highest payroll
      if (payrollByState[state] > highestPayroll) {
        highestPayroll = payrollByState[state];
        highestPayrollState = state;
      }
    }
    //   console.log(payrollByState);
    return highestPayrollState;
  };

  fetchRTIAUserStatusData = async (partitionKey) => {
    if (!partitionKey) {
      return 0;
    }
    const params = {
      TableName: "RTIAUserStatusTable",
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
        const totalPremium =
          data.Items[0]?.carrier_location_data?.carrier_k
            ?.total_standard_premium || "NULL";
        return totalPremium;
      } else {
        return 0;
      }
    } catch (error) {
      console.error("Error fetching E3 user status data:", error);
      throw new Error("Error fetching user status data");
    }
  };

  rtiaCalculate = async (item) => {
    const obj = {};
    obj["Unique Id"] = item?.user_email_id;
    obj["CompanyName"] = item?.companyProfile?.companyName?.value || "";
    obj["Created Date"] = item?.uploadTimestamp
      ? moment(item?.uploadTimestamp, ["x"]).format("MM-DD-YYYY")
      : "";
    obj["Total Payroll"] = item?.childrenLoc
      ? this.payrollCalculation(item?.childrenLoc)
      : 0;
    obj["GoverningCC"] = item?.childrenLoc
      ? this.getGoverningClassCode(item?.childrenLoc)
      : "NULL";
    obj["GoverningState"] = item?.childrenLoc
      ? this.getGoverningState(item?.childrenLoc)
      : "NULL";
    let status;
    if (item?.status === "quote_generated" && !item?.submissionsaved) {
      status = "Price Indication";
    } else if (item?.status === "quote_generated" && item?.submissionsaved) {
      status = "Submitted";
    } else {
      status = "-";
    }
    obj["Status"] = status;
    obj["selectedPEO"] = item?.companyProfile?.peo?.value || "";
    obj["Total Premium"] = await this.fetchRTIAUserStatusData(
      item?.user_email_id
    );
    console.log(obj);
    return obj;
  };

  async downloadRTIAData() {
    const finalResponse = [];
    const params = {
      TableName: "RTIAUserTable",
    };

    try {
      let data;
      do {
        data = await docClient.send(new ScanCommand(params));
        for (const item of data.Items) {
          finalResponse.push(await this.rtiaCalculate(item));
        }
        params.ExclusiveStartKey = data.LastEvaluatedKey;
      } while (typeof data.LastEvaluatedKey !== "undefined");
      return finalResponse;
    } catch (error) {
      console.error("Error fetching items from DynamoDB:", error);
      return [];
    }
  }
  //------------RTIA END HERE-----------------//

  //------------LIBERTATE START HERE-----------//

  libCalculate = async (item) => {
    const obj = {};
    obj["CompanyName"] = item?.companyProfile?.companyName?.value || "";
    obj["FEIN"] = item?.companyProfile?.fein?.value || "";
    let status;
    const dbStatus = item?.status || "";
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

    obj["Created Date"] = item?.createdDate
      ? moment(item?.createdDate, ["x"]).format("MM-DD-YYYY")
      : "";
    console.log(obj);
    return obj;
  };

  async downloadLibertateData() {
    const libResponse = [];
    const params = {
      TableName: "Icomp2UserTable",
      IndexName: "secondary_index_hash_key-uploadTimestamp-index",
      KeyConditionExpression: "#sihk=:sihk",
      ScanIndexForward: false,
      ExpressionAttributeNames: {
        "#sihk": "secondary_index_hash_key",
      },
      ExpressionAttributeValues: {
        ":sihk": "true",
      },
    };

    try {
      let data;
      do {
        data = await docClient.send(new QueryCommand(params));
        for (const item of data.Items) {
          libResponse.push(await this.libCalculate(item));
        }
        params.ExclusiveStartKey = data.LastEvaluatedKey;
      } while (typeof data.LastEvaluatedKey !== "undefined");

      return libResponse;
    } catch (error) {
      console.error("Error fetching items from DynamoDB:", error);
    }
  }

  async getInstanceSubmissionCountData(type, request, reply) {
    try {
      if (type === "e3") {
        const response = await this.downloadE3Data(type);
        reply.send(response);
      } else if (type === "extensis") {
        const response = await this.downloadExtensisData();
        reply.send(response);
      } else if (type === "ies") {
        const response = await this.downloadIESData(type);
        reply.send(response);
      } else if (type === "rtia") {
        const response = await this.downloadRTIAData();
        reply.send(response);
      } else if (type === "libertate") {
        const response = await this.downloadLibertateData();
        reply.send(response);
      }
    } catch (error) {
      console.error(`Error while Fetching ${request.params.type} data:`, error);
      return [];
    }
  }
}

module.exports = new instanceCountService();
