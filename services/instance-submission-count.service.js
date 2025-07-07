const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const {
  DynamoDBDocumentClient,
  ScanCommand,
  QueryCommand,
} = require("@aws-sdk/lib-dynamodb");
const moment = require("moment");
const client = new DynamoDBClient({ region: "us-east-1" });
const { GetObjectCommand, PutObjectCommand, HeadObjectCommand, S3Client } = require("@aws-sdk/client-s3");
const docClient = DynamoDBDocumentClient.from(client);
const s3Client = new S3Client({
  region: "us-east-1",
});
const LOOP_LIMIT = 3;
const BUCKET_NAME = "e3-quote-json-docs";

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
  async hasFileGenerated(params) {
  try {

    const command = new HeadObjectCommand(params);
    await s3Client.send(command);

    return true;
  } catch (err) {
    console.log(err);
    return false;
  }
  }
  streamToString = (stream) => {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
    stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
    stream.on("error", reject);
  });
  };
  async loopOverBucketAndCheckForFile(params, i = 0) {
    let returnVal = false;
    await new Promise((resolve, reject) => {
  
      if (i >= LOOP_LIMIT) reject("Error");
  
      setTimeout(async () => {
        let fileExists = this.hasFileGenerated(params);
  
        if (fileExists) resolve("found");
        else if (i < LOOP_LIMIT) resolve("continue");
        else resolve("not found");
      }, 3000);
    })
      .then(res => {
        console.log("res: ", res);
        if (res === "not found") {
          returnVal = false;
        } else if (res === "continue") {
          returnVal = this.loopOverBucketAndCheckForFile(params, i + 1);
        } else if (res === "found") {
          returnVal = true;
        }
      })
      .catch(err => { console.log(err); returnVal = false; });
    return returnVal;
  }
  async fetchFroms3  (key) {
    try {
      const PARAM_TO_BUCKET = {
        Bucket: BUCKET_NAME,
        Key: key,
      };
  
      let checkFile = await this.loopOverBucketAndCheckForFile(PARAM_TO_BUCKET, 0);
  
      if (!checkFile) throw new Error("File doesnot exist");
  
      const command = new GetObjectCommand(PARAM_TO_BUCKET);
  
      const data = await s3Client.send(command);
      const jsonQuotesData = await this.streamToString(data?.Body);
  
      return { data: jsonQuotesData, error: null };
  
    } catch (error) {
      console.log(error);
      return { data: null, error: "Error" }
    }
  }
  getE3TotalPremium =async (obj,key)=>{
    if (obj?.["keyToStorage"]) {
      let { data, error } = await this.fetchFroms3(obj["keyToStorage"]);
      let dataKey=key.split("+")[1]
      // console.log("Suraj 0098=====>",JSON.parse(data)?.[dataKey]?.["e3hr"])
      if (error) {
        console.log(error);
        return null
      };
      return JSON.parse(data)?.[dataKey]?.["e3hr"];
    } else {
      return obj;
    }
  }
  // Fetch E3 HR Data from User Status Data
  fetchE3UserStatusData = async (partitionKey) => {
    if (!partitionKey) {
      return 0;
    }
    let selectedCarrier= await this.fetchE3CarrierSelected(partitionKey)
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
      if (data.Items[0]?.carrier_location_data) {
        const jsonData = await this.getE3TotalPremium(
          data.Items[0].carrier_location_data, data.Items[0].uuid_carrier
        );
        let teap;
        if(jsonData&&selectedCarrier){
          teap=jsonData[selectedCarrier]?.total_estimated_annual_premium||0
        }
        return teap;
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
        const carrier = data.Items[0]?.tableCarrierSelect
          ? data.Items[0]?.tableCarrierSelect
          : "Null";
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
      ? new Date(date * 1000)
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
    const carrierMap = {
      carrier_az: "Arch",
      carrier_ba: "SUNZ",
      carrier_bc: "Prescient",
    };
    obj["Unique Id"] = item?.user_email_id;
    obj["CompanyName"] = item?.companyProfile?.companyName?.value || "";
    obj["FEIN"] = item?.companyProfile?.fein?.value || "";
    obj["TotalPremium"] = await this.fetchE3UserStatusData(
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
                    ? moment(Number(item.uploadTimestamp))
                        .utcOffset("-0500")
                        .format("M/D/YY")
                    : "-"
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
    obj["QuoteDataDate"] = item?.quoteData?.date
      ? new Date(parseInt(item.quoteData?.date))
      : null;
     let selectedCarrier= await this.fetchE3CarrierSelected(
      item?.user_email_id
    );
    obj["SelectedCarrier"]=selectedCarrier? carrierMap[selectedCarrier]:selectedCarrier
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
    ? new Date(parseInt(item.uploadTimestamp))
    : ""
    
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
      ? moment.unix(date).utcOffset("+05:30").format("MM-DD-YYYY")
      : "";
      return e3CreatedDate;
    } catch (error) {
      console.error("Error fetching items:", error);
      return [];
    }
  };
  iesCalculate = async (item, instanceType) => {
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
      ? new Date(parseInt(item.createdDate))
      : null;

    let lossRunData=await this.fetchIESpibitOCR(item?.user_email_id)
    if(lossRunData){
      obj["LossRunUpload"]="Yes"
      obj["LossRunDate"] = lossRunData
    }else{
      obj["LossRunUpload"]="No"
      obj["LossRunDate"] ="NULL"
    }
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
          iesResponse.push(await this.iesCalculate(item, instanceType));
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
      ? new Date(parseInt(item.uploadTimestamp))
      : null;
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
    obj["Unique Id"] = item?.user_email_id || "";
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
      ? new Date(parseInt(item.createdDate))
      : null;
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
          if(item?.origin_instance !== "ies"){
            libResponse.push(await this.libCalculate(item));
          }
          
        }
        params.ExclusiveStartKey = data.LastEvaluatedKey;
      } while (typeof data.LastEvaluatedKey !== "undefined");

      return libResponse;
    } catch (error) {
      console.error("Error fetching items from DynamoDB:", error);
    }
  }
  //------------LIBERTATE END HERE-----------//

  //------------FOURTH START HERE-----------//
  fetchFourthUserStatusData = async (partitionKey) => {
    if (!partitionKey) {
      return 0;
    }
    const params = {
      TableName: "FourthUserStatusTable",
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
          data.Items[0]?.carrier_location_data
            ?.total_estimated_annual_premium || 0;
        return totalPremium;
      } else {
        return 0;
      }
    } catch (error) {
      console.error("Error fetching E3 user status data:", error);
      throw new Error("Error fetching user status data");
    }
  };

  fourthCalculate = async (item) => {
    const obj = {};
    obj["Unique Id"] = item?.id || "";
    obj["CompanyName"] = item?.companyProfile?.companyName?.value || "";
    obj["Created Date"] = item?.created_timestamp
      ? new Date(parseInt(item.created_timestamp))
      : null;
    obj["Total Payroll"] = item?.currProspect?.childrenLoc
      ? this.payrollCalculation(item?.currProspect?.childrenLoc)
      : 0;
    obj["Source"] = item?.origin === "salesforce" ? "Salesforce" : "Insurecomp";
    obj["Status"] =
      item?.formStage === "one" || item.formStage === "two"
        ? "In Progress"
        : item?.formStage === "three" && item?.isSubmitted
          ? "Submitted"
          : "Quote Generated";
    obj["Total Premium"] = await this.fetchFourthUserStatusData(item?.id);
    return obj;
  };

  async downloadFourthData() {
    const fourthResponse = [];
    const params = {
      TableName: "FourthSalesPersonData",
    };

    try {
      let data;
      do {
        data = await client.send(new ScanCommand(params));
        for (const item of data.Items) {
          fourthResponse.push(await this.fourthCalculate(item));
        }
        params.ExclusiveStartKey = data.LastEvaluatedKey;
      } while (typeof data.LastEvaluatedKey !== "undefined");
      return fourthResponse;
    } catch (error) {
      console.error("Error scanning table:", error);
      throw error;
    }
  }
  //------------FOURTH END HERE-----------//

  //------------Get Instance Submission Count Data-----------//
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
      } else if (type === "fourth") {
        const response = await this.downloadFourthData();
        reply.send(response);
      }
    } catch (error) {
      console.error(`Error while Fetching ${request.params.type} data:`, error);
      return [];
    }
  }
}

module.exports = new instanceCountService();
