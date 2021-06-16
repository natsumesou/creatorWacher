import * as functions from "firebase-functions";
import {DocumentSnapshot} from "firebase-functions/lib/providers/firestore";
import {Change, EventContext} from "firebase-functions";
import {BigQuery} from "@google-cloud/bigquery";

export const ChangeType = {
  CREATE: "create",
  UPDATE: "update",
  DELETE: "delete",
};
type ChangeType = typeof ChangeType[keyof typeof ChangeType];

const dataset = "channels";

export const exportStreamsToBigQuery = async (change: Change<DocumentSnapshot>, context: EventContext) => {
  const channel = await change.after.ref.parent.parent?.get();
  if (!channel || !channel.exists) {
    return;
  }

  const changeType = getChangeType(change);
  await migrateStreamsToBigQuery(channel, [change.after], changeType);
};

export const migrateStreamsToBigQuery = async (channel: DocumentSnapshot, snapshots: DocumentSnapshot[], changeType: ChangeType) => {
  const projectId = process.env.GCLOUD_PROJECT;
  const bigQuery = new BigQuery({projectId: projectId});
  const table = "videos";

  if (changeType === ChangeType.DELETE) {
    const values = snapshots.map((snapshot) => `"${snapshot.id}"` ).join(",");
    const query = `DELETE \`${projectId}.${dataset}.${table}\` WHERE documentId in (${values})`;
    return await exec(bigQuery, query);
  }

  if (changeType === ChangeType.CREATE) {
    const values = snapshots.map((snapshot) => buildStreamQueryValues(snapshot, channel));
    const columns = [
      "chatAvailable", "chatCount", "chatDisabled", "gameTitle", "publishedAt", "streamLengthSec", "subscribeCount", "superChatAmount", "title", "viewCount", "updatedAt", "createdAt", "superChatCount",
    ];
    const extraCol = ["id", "channelTitle", "category", "channelId", "documentId"];
    const requireCol = columns.filter(((col) => snapshots[0].get(col) !== undefined));
    const query = `INSERT \`${projectId}.${dataset}.${table}\` (${requireCol.concat(extraCol).join(",")}) VALUES ${values.join(",")}`;
    functions.logger.log("CREATE stream: " + query);
    return await exec(bigQuery, query);
  }

  if (changeType === ChangeType.UPDATE) {
    for await (const snapshot of snapshots) {
      const query = `UPDATE \`${projectId}.${dataset}.${table}\` ${snapshot.get("chatAvailable") ? `SET chatAvailable = ${snapshot.get("chatAvailable")},` : ""} chatCount = ${snapshot.get("chatCount")}, ${snapshot.get("chatDisabled") ? `chatDisabled = ${snapshot.get("chatDisabled")},` : ""} ${snapshot.get("gameTitle") ? `gameTitle = "${snapshot.get("gameTitle").replace(/"/g, "\\\"")}",` : ""} ${snapshot.get("publishedAt") ? `publishedAt = TIMESTAMP("${snapshot.get("publishedAt").toDate().toISOString()}"),` : ""} streamLengthSec = ${snapshot.get("streamLengthSec")}, subscribeCount = ${snapshot.get("subscribeCount")}, superChatAmount = ${snapshot.get("superChatAmount")}, ${snapshot.get("title") ? `title = "${snapshot.get("title").replace(/"/g, "\\\"")}",` : ""} viewCount = ${snapshot.get("viewCount")}, ${snapshot.get("updatedAt") ? `updatedAt = TIMESTAMP("${snapshot.get("updatedAt").toDate().toISOString()}"),` : ""} ${snapshot.get("createdAt") ? `createdAt = TIMESTAMP("${snapshot.get("createdAt").toDate().toISOString()}"),` : ""} superChatCount = ${snapshot.get("superChatCount")}, id = "${snapshot.id}", channelTitle = ${channel.get("title") ? `"${channel.get("title").replace(/"/g, "\\\"")}",` : ""} category = "${channel.get("category")}", channelId = "${channel.id}", documentId = "${snapshot.id}" WHERE documentId = "${snapshot.id}"`;
      functions.logger.log("UPDATE stream: " + query);
      return await exec(bigQuery, query);
    }
  }
};

const buildStreamQueryValues = (snapshot: DocumentSnapshot, channel: DocumentSnapshot) => {
  return `(${snapshot.get("chatAvailable") ? `${snapshot.get("chatAvailable")},` : ""} ${snapshot.get("chatCount")}, ${snapshot.get("chatDisabled") ? `${snapshot.get("chatDisabled")},` : ""} ${snapshot.get("gameTitle") ? `"${snapshot.get("gameTitle").replace(/"/g, "\\\"")}"` : "NULL"}, ${snapshot.get("publishedAt") ? `TIMESTAMP("${snapshot.get("publishedAt").toDate().toISOString()}")` : "NULL"}, ${snapshot.get("streamLengthSec")}, ${snapshot.get("subscribeCount")}, ${snapshot.get("superChatAmount")}, ${snapshot.get("title") ? `"${snapshot.get("title").replace(/"/g, "\\\"")}"` : "NULL"}, ${snapshot.get("viewCount")}, ${snapshot.get("updatedAt") ? `TIMESTAMP("${snapshot.get("updatedAt").toDate().toISOString()}")` : "NULL"}, ${snapshot.get("createdAt") ? `TIMESTAMP("${snapshot.get("createdAt").toDate().toISOString()}"),`: "NULL"} ${snapshot.get("superChatCount")}, "${snapshot.id}", "${channel.get("title") ? channel.get("title").replace(/"/g, "\\\"") : "NULL"}", "${channel.get("category")}", "${channel.id}", "${snapshot.id}")`;
};

export const exportSuperChatsToBigQuery = async (change: Change<DocumentSnapshot>, context: EventContext) => {
  const changeType = getChangeType(change);
  const channelId = context.params.channelId;

  await migrateSuperChatsToBigQuery([change.after], channelId, changeType);
};

export const migrateSuperChatsToBigQuery = async (snapshots: DocumentSnapshot[], channelId: string, changeType: ChangeType) => {
  const projectId = process.env.GCLOUD_PROJECT;
  const bigQuery = new BigQuery({projectId: projectId});
  const table = "superChats";

  if (changeType === ChangeType.DELETE) {
    const values = snapshots.map((snapshot) => `"${snapshot.id}"` ).join(",");
    const query = `DELETE \`${projectId}.${dataset}.${table}\` WHERE documentId in (${values})`;
    return await exec(bigQuery, query);
  }

  if (changeType === ChangeType.CREATE) {
    const values = snapshots.map((snapshot) => buildSuperChatQueryValues(snapshot, channelId));
    const columns = [
      "supporterChannelId", "supporterDisplayName", "amount", "amountText", "unit", "thumbnail", "paidAt",
    ];
    const extraCol = ["documentId", "channelId", "videoId"];
    const requireCol = columns.filter((col) => snapshots[0].get(col) !== undefined);
    const query = `INSERT \`${projectId}.${dataset}.${table}\` (${requireCol.concat(extraCol).join(",")}) VALUES ${values.join(",")}`;
    functions.logger.log("CREATE SuperChats: " + query);
    return await exec(bigQuery, query);
  }

  if (changeType === ChangeType.UPDATE) {
    for await (const snapshot of snapshots) {
      const query = `UPDATE \`${projectId}.${dataset}.${table}\` SET videoId = "${snapshot.ref.parent.id}", supporterChannelId = "${snapshot.get("supporterChannelId")}", supporterDisplayName = "${snapshot.get("supporterDisplayName").replace(/"/g, "\\\"")}", amount = ${snapshot.get("amount")}, amountText = "${snapshot.get("amountText")}", unit = "${snapshot.get("unit")}", thumbnail = ${snapshot.get("thumbnail") ? `"${snapshot.get("thumbnail")}"` : "NULL"}, paidAt = TIMESTAMP("${snapshot.get("paidAt").toDate().toISOString()}"), documentId = "${snapshot.id}", channelId = "${channelId}" WHERE documentId = "${snapshot.id}"`;
      functions.logger.log("UPDATE SuperChats: " + query);
      return await exec(bigQuery, query);
    }
  }
};

const buildSuperChatQueryValues = (snapshot: DocumentSnapshot, channelId: string) => {
  return `("${snapshot.get("supporterChannelId")}", "${snapshot.get("supporterDisplayName").replace(/"/g, "\\\"")}", ${snapshot.get("amount")}, "${snapshot.get("amountText")}", "${snapshot.get("unit")}", ${snapshot.get("thumbnail") ? `"${snapshot.get("thumbnail")}"` : "NULL"}, TIMESTAMP("${snapshot.get("paidAt").toDate().toISOString()}"), "${snapshot.id}", "${channelId}", "${snapshot.ref.parent.id}")`;
};

const exec = async (bigQuery: BigQuery, query: string, retry = true) => {
  const options = {
    query: query,
    location: "asia-northeast1",
  };
  try {
    const [job] = await bigQuery.createQueryJob(options);
    await job.getQueryResults();
  } catch (e) {
    if (retry && isRetryableInsertionError(e)) {
      retry = false;
      await exec(bigQuery, query, retry);
    }
    throw e;
  }
};

const isRetryableInsertionError = (e: any) => {
  let isRetryable = true;
  const expectedErrors = [
    {
      message: "no such field.",
      location: "document_id",
    },
  ];
  if (
    e.response &&
    e.response.insertErrors &&
    e.response.insertErrors.errors
  ) {
    const errors = e.response.insertErrors.errors;
    errors.forEach((error: any) => {
      let isExpected = false;
      expectedErrors.forEach((expectedError) => {
        if (
          error.message === expectedError.message &&
          error.location === expectedError.location
        ) {
          isExpected = true;
        }
      });
      if (!isExpected) {
        isRetryable = false;
      }
    });
  }
  return isRetryable;
};

const getChangeType = (change: Change<DocumentSnapshot>) => {
  if (!change.after.exists) {
    return ChangeType.DELETE;
  }
  if (!change.before.exists) {
    return ChangeType.CREATE;
  }
  return ChangeType.UPDATE;
};
