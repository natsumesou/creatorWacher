import {DocumentSnapshot} from "firebase-functions/lib/providers/firestore";
import {Change, EventContext} from "firebase-functions";
import {BigQuery} from "@google-cloud/bigquery";

const ChangeType = {
  CREATE: "create",
  UPDATE: "update",
  DELETE: "delete",
};

const dataset = "channels";

export const exportStreamsToBigQuery = async (change: Change<DocumentSnapshot>, context: EventContext) => {
  const projectId = process.env.GCLOUD_PROJECT;
  const bigQuery = new BigQuery({projectId: projectId});
  const table = "videos";
  const changeType = getChangeType(change);
  const documentId = getDocumentId(change);

  if (changeType === ChangeType.DELETE) {
    const query = `DELETE \`${projectId}.${dataset}.${table}\` WHERE documentId = "${documentId}"`;
    return await exec(bigQuery, query);
  }

  const channel = await change.after.ref.parent.parent?.get();
  if (!channel || !channel.exists) {
    return;
  }

  if (changeType === ChangeType.CREATE) {
    const query = `INSERT \`${projectId}.${dataset}.${table}\` (chatAvailable, chatCount, chatDisabled, gameTitle, publishedAt, streamLengthSec, subscribeCount, superChatAmount, title, viewCount, startedAt, createdAt, superChatCount, id, channelTitle, category, channelId, documentId) VALUES ("${change.after.get("chatAvailable")}", ${change.after.get("chatCount")}, ${change.after.get("chatDisabled")}, "${change.after.get("gameTitle").replace(/"/g, "\\\"")}", TIMESTAMP("${change.after.get("publishedAt").getDate().toISOString()}"), ${change.after.get("streamLengthSec")}, ${change.after.get("subscribeCount")}, ${change.after.get("superChatAmount")}, "${change.after.get("title").replace(/"/g, "\\\"")}", ${change.after.get("viewCount")}, TIMESTAMP("${change.after.get("startedAt").toDate().toISOString()}") AND updatedAt = TIMESTAMP("${change.after.get("updatedAt").toDate().toISOString()}"), TIMESTAMP("${change.after.get("createdAt").toDate().toISOString()}"), ${change.after.get("superChatCount")}, "${change.after.id}", "${channel.get("title").replace(/"/g, "\\\"")}", "${channel.get("category")}", "${channel.id}", "${documentId}")`;
    return await exec(bigQuery, query);
  }

  if (changeType === ChangeType.UPDATE) {
    const query = `UPDATE \`${projectId}.${dataset}.${table}\` SET chatAvailable = "${change.after.get("chatAvailable")}" AND chatCount = ${change.after.get("chatCount")} AND chatDisabled = ${change.after.get("chatDisabled")} AND gameTitle = "${change.after.get("gameTitle").replace(/"/g, "\\\"")}" AND publishedAt = TIMESTAMP("${change.after.get("publishedAt").getDate().toISOString()}") AND streamLengthSec = ${change.after.get("streamLengthSec")} AND subscribeCount = ${change.after.get("subscribeCount")} AND superChatAmount = ${change.after.get("superChatAmount")} AND title = "${change.after.get("title").replace(/"/g, "\\\"")}" AND viewCount = ${change.after.get("viewCount")} AND startedAt = TIMESTAMP("${change.after.get("startedAt").toDate().toISOString()}") AND updatedAt = TIMESTAMP("${change.after.get("updatedAt").toDate().toISOString()}") AND createdAt = TIMESTAMP("${change.after.get("createdAt").toDate().toISOString()}") AND superChatCount = ${change.after.get("superChatCount")} AND id = "${change.after.id}" AND channelTitle = ${channel.get("title").replace(/"/g, "\\\"")}" AND category = "${channel.get("category")}" AND channelId = "${channel.id}" AND documentId = "${documentId}" WHERE documentId = "${documentId}"`;
    return await exec(bigQuery, query);
  }
};

export const exportSuperChatsToBigQuery = async (change: Change<DocumentSnapshot>, context: EventContext) => {
  const projectId = process.env.GCLOUD_PROJECT;
  const bigQuery = new BigQuery({projectId: projectId});
  const table = "superChats";
  const changeType = getChangeType(change);
  const documentId = getDocumentId(change);
  const channelId = context.params.channelId;
  const videoId = context.params.videoId;

  if (changeType === ChangeType.DELETE) {
    const query = `DELETE \`${projectId}.${dataset}.${table}\` WHERE documentId = "${documentId}"`;
    return await exec(bigQuery, query);
  }

  const channel = await change.after.ref.parent.parent?.parent.parent?.get();
  if (!channel || !channel.exists) {
    return;
  }

  if (changeType === ChangeType.CREATE) {
    const query = `INSERT \`${projectId}.${dataset}.${table}\` (videoId, supporterChannelId, supporterDisplayName, amount, amountText, unit, thumbnail, paidAt, documentId, channelId) VALUES ("${videoId}", "${change.after.get("supporterChannelId")}", "${change.after.get("supporterDisplayName").replace(/"/g, "\\\"")}", ${change.after.get("amount")}, "${change.after.get("amountText")}", "${change.after.get("unit")}", "${change.after.get("thumbnail")}", TIMESTAMP("${change.after.get("paidAt").toDate().toISOString()}"), "${documentId}", "${channelId}")`;
    return await exec(bigQuery, query);
  }

  if (changeType === ChangeType.UPDATE) {
    const query = `UPDATE \`${projectId}.${dataset}.${table}\` SET videoId = "${videoId}" AND supporterChannelId = "${change.after.get("supporterChannelId")}" AND supporterDisplayName = ${change.after.get("supporterDisplayName").replace(/"/g, "\\\"")} AND amount = ${change.after.get("amount")} AND amountText = "${change.after.get("amountText")}" AND unit = "${change.after.get("unit")}" AND thumbnail = "${change.after.get("thumbnail")}" AND paidAt = TIMESTAMP("${change.after.get("paidAt").toDate().toISOString()}") AND documentId = "${documentId}" AND channelId = "${channelId}" WHERE documentId = "${documentId}"`;
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
