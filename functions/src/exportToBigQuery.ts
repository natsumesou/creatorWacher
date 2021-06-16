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
  const changeType = getChangeType(change);
  const documentId = getDocumentId(change);
  await migrateStreamsToBigQuery(change.after, documentId, changeType);
};

export const migrateStreamsToBigQuery = async (snapshot: DocumentSnapshot, documentId: string, changeType: ChangeType) => {
  const projectId = process.env.GCLOUD_PROJECT;
  const bigQuery = new BigQuery({projectId: projectId});
  const table = "videos";

  if (changeType === ChangeType.DELETE) {
    const query = `DELETE \`${projectId}.${dataset}.${table}\` WHERE documentId = "${documentId}"`;
    return await exec(bigQuery, query);
  }

  const channel = await snapshot.ref.parent.parent?.get();
  if (!channel || !channel.exists) {
    return;
  }

  if (changeType === ChangeType.CREATE) {
    const query = `INSERT \`${projectId}.${dataset}.${table}\` (chatAvailable, chatCount, chatDisabled, gameTitle, publishedAt, streamLengthSec, subscribeCount, superChatAmount, title, viewCount, createdAt, superChatCount, id, channelTitle, category, channelId, documentId) VALUES (${snapshot.get("chatAvailable")}, ${snapshot.get("chatCount")}, ${snapshot.get("chatDisabled")}, ${snapshot.get("gameTitle") ? `"${snapshot.get("gameTitle").replace(/"/g, "\\\"")}"` : "NULL"}, ${snapshot.get("publishedAt") ? `TIMESTAMP("${snapshot.get("publishedAt").toDate().toISOString()}")` : "NULL"}, ${snapshot.get("streamLengthSec")}, ${snapshot.get("subscribeCount")}, ${snapshot.get("superChatAmount")}, ${snapshot.get("title") ? `"${snapshot.get("title").replace(/"/g, "\\\"")}"` : "NULL"}, ${snapshot.get("viewCount")}, ${snapshot.get("updatedAt") ? `TIMESTAMP("${snapshot.get("updatedAt").toDate().toISOString()}")` : "NULL"}, TIMESTAMP("${snapshot.get("createdAt").toDate().toISOString()}"), ${snapshot.get("superChatCount")}, "${snapshot.id}", "${channel.get("title").replace(/"/g, "\\\"")}", "${channel.get("category")}", "${channel.id}", "${documentId}")`;
    return await exec(bigQuery, query);
  }

  if (changeType === ChangeType.UPDATE) {
    const query = `UPDATE \`${projectId}.${dataset}.${table}\` SET chatAvailable = ${snapshot.get("chatAvailable")}, chatCount = ${snapshot.get("chatCount")}, chatDisabled = ${snapshot.get("chatDisabled")}, ${snapshot.get("gameTitle") ? `gameTitle = "${snapshot.get("gameTitle").replace(/"/g, "\\\"")}",` : ""} ${snapshot.get("publishedAt") ? `publishedAt = TIMESTAMP("${snapshot.get("publishedAt").toDate().toISOString()}"),` : ""} streamLengthSec = ${snapshot.get("streamLengthSec")}, subscribeCount = ${snapshot.get("subscribeCount")}, superChatAmount = ${snapshot.get("superChatAmount")}, ${snapshot.get("title") ? `title = "${snapshot.get("title").replace(/"/g, "\\\"")}",` : ""} viewCount = ${snapshot.get("viewCount")}, ${snapshot.get("updatedAt") ? `updatedAt = TIMESTAMP("${snapshot.get("updatedAt").toDate().toISOString()}"),` : ""} createdAt = TIMESTAMP("${snapshot.get("createdAt").toDate().toISOString()}"), superChatCount = ${snapshot.get("superChatCount")}, id = "${snapshot.id}", channelTitle = "${channel.get("title").replace(/"/g, "\\\"")}", category = "${channel.get("category")}", channelId = "${channel.id}", documentId = "${documentId}" WHERE documentId = "${documentId}"`;
    return await exec(bigQuery, query);
  }
};

export const exportSuperChatsToBigQuery = async (change: Change<DocumentSnapshot>, context: EventContext) => {
  const changeType = getChangeType(change);
  const documentId = getDocumentId(change);
  const channelId = context.params.channelId;
  const videoId = context.params.videoId;

  await migrateSuperChatsToBigQuery(change.after, documentId, channelId, videoId, changeType);
};

export const migrateSuperChatsToBigQuery = async (snapshot: DocumentSnapshot, documentId: string, channelId: string, videoId: string, changeType: ChangeType) => {
  const projectId = process.env.GCLOUD_PROJECT;
  const bigQuery = new BigQuery({projectId: projectId});
  const table = "superChats";

  if (changeType === ChangeType.DELETE) {
    const query = `DELETE \`${projectId}.${dataset}.${table}\` WHERE documentId = "${documentId}"`;
    return await exec(bigQuery, query);
  }

  const channel = await snapshot.ref.parent.parent?.parent.parent?.get();
  if (!channel || !channel.exists) {
    return;
  }

  if (changeType === ChangeType.CREATE) {
    const query = `INSERT \`${projectId}.${dataset}.${table}\` (videoId, supporterChannelId, supporterDisplayName, amount, amountText, unit, thumbnail, paidAt, documentId, channelId) VALUES ("${videoId}", "${snapshot.get("supporterChannelId")}", "${snapshot.get("supporterDisplayName").replace(/"/g, "\\\"")}", ${snapshot.get("amount")}, "${snapshot.get("amountText")}", "${snapshot.get("unit")}", ${snapshot.get("thumbnail") ? `"${snapshot.get("thumbnail")}"` : "NULL"}, TIMESTAMP("${snapshot.get("paidAt").toDate().toISOString()}"), "${documentId}", "${channelId}")`;
    return await exec(bigQuery, query);
  }

  if (changeType === ChangeType.UPDATE) {
    const query = `UPDATE \`${projectId}.${dataset}.${table}\` SET videoId = "${videoId}", supporterChannelId = "${snapshot.get("supporterChannelId")}", supporterDisplayName = "${snapshot.get("supporterDisplayName").replace(/"/g, "\\\"")}", amount = ${snapshot.get("amount")}, amountText = "${snapshot.get("amountText")}", unit = "${snapshot.get("unit")}", thumbnail = ${snapshot.get("thumbnail") ? `"${snapshot.get("thumbnail")}"` : "NULL"}, paidAt = TIMESTAMP("${snapshot.get("paidAt").toDate().toISOString()}"), documentId = "${documentId}", channelId = "${channelId}" WHERE documentId = "${documentId}"`;
    return await exec(bigQuery, query);
  }
};

const exec = async (bigQuery: BigQuery, query: string) => {
  const options = {
    query: query,
    location: "asia-northeast1",
  };
  try {
    const [job] = await bigQuery.createQueryJob(options);
    await job.getQueryResults();
  } catch (err) {
    if (err.code === 400) {
      console.error(`documentId: クエリの実行に失敗しました。\n${query}\n${err.message}`);
    } else {
      throw err;
    }
  }
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

const getDocumentId = (change: Change<DocumentSnapshot>) => {
  if (change.after.exists) {
    return change.after.id;
  }
  return change.before.id;
};
