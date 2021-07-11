import * as functions from "firebase-functions";
import {DocumentSnapshot} from "firebase-functions/lib/providers/firestore";
import {Change, EventContext} from "firebase-functions";
import {BigQuery} from "@google-cloud/bigquery";
import * as admin from "firebase-admin";
import {sleep} from "./lib/utility";

export const ChangeType = {
  CREATE: "create",
  UPDATE: "update",
  DELETE: "delete",
};
type ChangeType = typeof ChangeType[keyof typeof ChangeType];

const errorHandler = (err: Error, message?: string|null) => {
  throw err;
};

const dataset = "channels";

export const exportStreamsToBigQuery = async (change: Change<DocumentSnapshot>, context: EventContext) => {
  try {
    const channel = await change.after.ref.parent.parent?.get();
    if (!channel || !channel.exists) {
      return;
    }

    const changeType = getChangeType(change);
    await migrateStreamsToBigQuery(channel, [change.after], changeType);
  } catch (err) {
    errorHandler(err);
  }
};

export const migrateStreamsToBigQuery = async (channel: DocumentSnapshot, snapshots: DocumentSnapshot[], changeType: ChangeType) => {
  try {
    const projectId = process.env.GCLOUD_PROJECT;
    const bigQuery = new BigQuery({projectId: projectId});
    const table = "videos";

    if (changeType === ChangeType.DELETE) {
      const values = snapshots.map((snapshot) => `"${snapshot.id}"` ).join(",");
      const query = `DELETE \`${projectId}.${dataset}.${table}\` WHERE documentId in (${values})`;
      return await exec(bigQuery, query).catch((err) => errorHandler(err, `can not delete channel: ${channel.id}`));
    }

    if (changeType === ChangeType.CREATE) {
      // VideoはすぐにUPDATEが必要な上に書き込み数が少ないのでDMLで処理する
      const values = snapshots.map((snapshot) => buildStreamQueryValues(snapshot, channel));
      const query = `INSERT \`${projectId}.${dataset}.${table}\` (chatAvailable, chatCount, chatDisabled, gameTitle, publishedAt, streamLengthSec, subscribeCount, superChatAmount, title, viewCount, updatedAt, createdAt, superChatCount, id, channelTitle, category, channelId, documentId) VALUES ${values.join(",")}`;
      return await exec(bigQuery, query).catch((err) => errorHandler(err, `can not inert channel: ${channel.id}`));
    }

    if (changeType === ChangeType.UPDATE) {
      for await (const snapshot of snapshots) {
        const query = `UPDATE \`${projectId}.${dataset}.${table}\` SET ${(snapshot.get("chatAvailable") !== undefined) ? `chatAvailable = ${snapshot.get("chatAvailable")},` : ""} chatCount = ${snapshot.get("chatCount")}, ${(snapshot.get("chatDisabled") !== undefined) ? `chatDisabled = ${snapshot.get("chatDisabled")},` : ""} ${snapshot.get("gameTitle") ? `gameTitle = "${snapshot.get("gameTitle").replace(/"/g, "\\\"")}",` : ""} ${snapshot.get("publishedAt") ? `publishedAt = TIMESTAMP("${snapshot.get("publishedAt").toDate().toISOString()}"),` : ""} streamLengthSec = ${snapshot.get("streamLengthSec")}, subscribeCount = ${snapshot.get("subscribeCount")}, superChatAmount = ${snapshot.get("superChatAmount").toFixed(9)}, ${snapshot.get("title") ? `title = "${snapshot.get("title").replace(/"/g, "\\\"")}",` : ""} viewCount = ${snapshot.get("viewCount")}, ${snapshot.get("updatedAt") ? `updatedAt = TIMESTAMP("${snapshot.get("updatedAt").toDate().toISOString()}"),` : ""} ${snapshot.get("createdAt") ? `createdAt = TIMESTAMP("${snapshot.get("createdAt").toDate().toISOString()}"),` : ""} superChatCount = ${snapshot.get("superChatCount")}, id = "${snapshot.id}", channelTitle = ${channel.get("title") ? `"${channel.get("title").replace(/"/g, "\\\"")}",` : ""} category = "${channel.get("category")}", channelId = "${channel.id}", documentId = "${snapshot.id}" WHERE documentId = "${snapshot.id}"`;
        return await exec(bigQuery, query).catch((err) => errorHandler(err, `can not update channel: ${channel.id}`));
      }
    }
  } catch (err) {
    errorHandler(err, `error occured channel: ${channel.id}`);
  }
};

const buildStreamQueryValues = (snapshot: DocumentSnapshot, channel: DocumentSnapshot) => {
  return `(${(snapshot.get("chatAvailable") !== undefined) ? `${snapshot.get("chatAvailable")}` : "NULL"}, ${snapshot.get("chatCount")}, ${(snapshot.get("chatDisabled") !== undefined) ? `${snapshot.get("chatDisabled")}` : "NULL"}, ${snapshot.get("gameTitle") ? `"${snapshot.get("gameTitle").replace(/"/g, "\\\"")}"` : "NULL"}, ${snapshot.get("publishedAt") ? `TIMESTAMP("${snapshot.get("publishedAt").toDate().toISOString()}")` : "NULL"}, ${snapshot.get("streamLengthSec")}, ${snapshot.get("subscribeCount")}, ${snapshot.get("superChatAmount").toFixed(9)}, ${snapshot.get("title") ? `"${snapshot.get("title").replace(/"/g, "\\\"")}"` : "NULL"}, ${snapshot.get("viewCount")}, ${snapshot.get("updatedAt") ? `TIMESTAMP("${snapshot.get("updatedAt").toDate().toISOString()}")` : "NULL"}, ${snapshot.get("createdAt") ? `TIMESTAMP("${snapshot.get("createdAt").toDate().toISOString()}"),`: "NULL"} ${snapshot.get("superChatCount")}, "${snapshot.id}", "${channel.get("title") ? channel.get("title").replace(/"/g, "\\\"") : "NULL"}", "${channel.get("category")}", "${channel.id}", "${snapshot.id}")`;
};

export const exportSuperChatsToBigQuery = async (change: Change<DocumentSnapshot>, context: EventContext) => {
  try {
    const changeType = getChangeType(change);
    const channelId = context.params.channelId;
    const videoId = context.params.videoId;

    await migrateSuperChatsToBigQuery([change.after], channelId, videoId, changeType);
  } catch (err) {
    errorHandler(err);
  }
};

export const migrateSuperChatsToBigQuery = async (snapshots: DocumentSnapshot[], channelId: string, videoId: string, changeType: ChangeType) => {
  const projectId = process.env.GCLOUD_PROJECT;
  const bigQuery = new BigQuery({projectId: projectId});
  const table = "superChats";

  if (changeType === ChangeType.CREATE) {
    const db = admin.firestore();
    const stream = await db.collection(`channels/${channelId}/streams`).doc(videoId).get().catch((err) => {
      functions.logger.error(err.message + "\n" + err.stack);
    });

    if (!stream || !stream.exists) {
      return;
    }

    const data = snapshots.map((snapshot) => {
      return {
        videoId: videoId,
        supporterChannelId: snapshot.get("supporterChannelId"),
        supporterDisplayName: snapshot.get("supporterDisplayName"),
        amount: snapshot.get("amount").toFixed(9), // 小数点以下9桁までしか保存できない
        amountText: snapshot.get("amountText"),
        originalAmount: parseFloat(snapshot.get("amountText").replace(/[^0-9.]+/g, "")).toFixed(9),
        unit: snapshot.get("unit"),
        thumbnail: snapshot.get("thumbnail") || null,
        paidAt: snapshot.get("paidAt").toDate(),
        documentId: snapshot.id,
        channelId: channelId,
        videoPublishedAt: stream.get("publishedAt").toDate(),
      };
    });
    // SuperChatは基本書き込みのみで変更なし。DMLだとLate Limitに引っかかって書き込みがコケるのでStreaming writeする。
    await bigQuery.dataset(dataset).table(table).insert(data).catch((err) => {
      const message = `superChat insert error: ${channelId}/streams/${videoId}/superChats \n${err.message}\n${JSON.stringify(err.errors)})`;
      functions.logger.error(message);
    });
  }

  if (changeType === ChangeType.UPDATE || changeType === ChangeType.DELETE) {
    snapshots.map((snapshot) => {
      throw new Error(`can not ${changeType} superchats ${channelId}/streams/${videoId}`);
    });
  }
};

const exec = async (bigQuery: BigQuery, query: string, retryCount = 0) => {
  const MAX_RETRY = 7;
  const options = {
    query: query,
    location: "asia-northeast1",
  };
  try {
    const [job] = await bigQuery.createQueryJob(options);
    await job.getQueryResults();
  } catch (e) {
    if (retryCount < MAX_RETRY) {
      await sleep(Math.exp(retryCount) * 10);
      await exec(bigQuery, query, ++retryCount);
    }
    throw e;
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
