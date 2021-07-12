import * as functions from "firebase-functions";
import {BigQuery} from "@google-cloud/bigquery";
import {upload} from "./lib/cloudStorage";
import {sleep} from "./lib/utility";

export const calcIndivisualSuperChats = async () => {
  const projectId = process.env.GCLOUD_PROJECT;
  const bigQuery = new BigQuery({projectId: projectId});

  // 実行タイミングの前日分までの月間スパチャ(個人のチャンネルごとのスパチャ金額)を集計
  const query = "SELECT  ranking.supporterChannelId,  ranking.supporterDisplayName,  ranking.thumbnail,  CASE WHEN ranking.superChatAmount is NULL THEN '0円' ELSE ranking.superChatAmount END as superChatAmount,  CASE WHEN ranking.totalSuperChatAmount is NULL THEN '0円' ELSE ranking.totalSuperChatAmount END AS totalSuperChatAmount,  ranking.channelId,  ranking.channelTitle,  video.videoIdFROM (  SELECT    sco.supporterChannelId,    sco.supporterDisplayName,    sco.thumbnail,    superChatAmount,    total.total AS totalSuperChatAmount,    process.channelId,    process.channelTitle  FROM (    SELECT      AS VALUE ARRAY_AGG(scb      ORDER BY        paidAt DESC      LIMIT        1)[    OFFSET      (0)]    FROM      `discord-315419.channels.superChats` AS scb    GROUP BY      supporterChannelId ) AS sco  LEFT JOIN (    SELECT      sc.supporterChannelId,      CONCAT(FORMAT(\"%'.0f\", SUM(sc.amount)), '円') AS superChatAmount,      sc.channelId,      channelTitle    FROM      `discord-315419.channels.superChats` AS sc    JOIN (      SELECT        channelId,        channelTitle      FROM        `discord-315419.channels.videos`      WHERE        category IN ('hololive',          'nijisanji')      GROUP BY        channelId,        channelTitle) AS channel    ON      sc.channelId = channel.channelId    WHERE videoPublishedAt < TIMESTAMP_SUB(TIMESTAMP(DATE_ADD(LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 DAY)), INTERVAL 1 DAY)), INTERVAL 4 HOUR)    AND videoPublishedAt >= TIMESTAMP_SUB(TIMESTAMP(DATE_SUB(DATE_ADD(LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 DAY)), INTERVAL 1 DAY), INTERVAL 1 MONTH)), INTERVAL 4 HOUR)    GROUP BY      supporterChannelId,      channelId,      sc.channelId,      channelTitle ) AS process  ON    sco.supporterChannelId = process.supporterChannelId  LEFT JOIN (    SELECT      supporterChannelId,      CONCAT(FORMAT(\"%'.0f\", SUM(amount)), '円') AS total,    FROM      `discord-315419.channels.superChats`    WHERE      videoPublishedAt < TIMESTAMP_SUB(TIMESTAMP(DATE_ADD(LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 DAY)), INTERVAL 1 DAY)), INTERVAL 4 HOUR)      AND videoPublishedAt >= TIMESTAMP_SUB(TIMESTAMP(DATE_SUB(DATE_ADD(LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 DAY)), INTERVAL 1 DAY), INTERVAL 1 MONTH)), INTERVAL 4 HOUR)    GROUP BY      supporterChannelId) AS total  ON    process.supporterChannelId = total.supporterChannelId ) AS rankingLEFT JOIN (  SELECT    *  FROM (    SELECT      supporterChannelId,      channelId,      videoId,      ROW_NUMBER() OVER (PARTITION BY channelId, supporterChannelId ORDER BY SUM(amount) DESC ) AS videoRank    FROM      `discord-315419.channels.superChats`    WHERE      videoPublishedAt < TIMESTAMP_SUB(TIMESTAMP(DATE_ADD(LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 DAY)), INTERVAL 1 DAY)), INTERVAL 4 HOUR)      AND videoPublishedAt >= TIMESTAMP_SUB(TIMESTAMP(DATE_SUB(DATE_ADD(LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 DAY)), INTERVAL 1 DAY), INTERVAL 1 MONTH)), INTERVAL 4 HOUR)    GROUP BY      supporterChannelId,      channelId,      videoId)  WHERE    videoRank = 1 ) AS video ON  ranking.supporterChannelId = video.supporterChannelId  AND ranking.channelId = video.channelId ORDER BY  ranking.supporterChannelId,  ranking.superChatAmount DESC";
  const rows = await exec(bigQuery, query).catch((err) => {
    functions.logger.error(`${err.message}\n${err.stack}`);
  });
  if (!rows) {
    return;
  }
  const group = rows.reduce((result: any, row: any) => {
    const groupId = getGroupId(row.supporterChannelId);
    if (result[groupId] === undefined) {
      result[groupId] = "";
    }
    result[groupId] += rowToTsv(row);
    return result;
  }, {});
  for await (const key of Object.keys(group)) {
    await upload(group[key], "monthly.tsv", `user/${key}/`, 3600);
  }
};

const getGroupId = (id: string) => {
  return id.slice(0, 3);
};

const rowToTsv = (row: any) => {
  return `${row.supporterChannelId}\t${row.supporterDisplayName}\t${row.thumbnail}\t${row.superChatAmount}\t${row.totalSuperChatAmount}\t${row.channelId}\t${row.channelTitle}\t${row.videoId}\n`;
};

const exec = async (bigQuery: BigQuery, query: string, retryCount = 0) => {
  const MAX_RETRY = 7;
  const options = {
    query: query,
    location: "asia-northeast1",
  };
  try {
    const [job] = await bigQuery.createQueryJob(options);
    const [rows] = await job.getQueryResults();
    return rows;
  } catch (e) {
    if (retryCount < MAX_RETRY) {
      await sleep(Math.exp(retryCount) * 10);
      await exec(bigQuery, query, ++retryCount);
    }
    throw e;
  }
};
