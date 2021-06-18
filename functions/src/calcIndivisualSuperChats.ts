import * as functions from "firebase-functions";
import {BigQuery} from "@google-cloud/bigquery";
import {Bot} from "./lib/discordNotify";
import {upload} from "./lib/cloudStorage";

const initializeBot = () => {
  return new Bot(
      functions.config().discord.hololive,
      functions.config().discord.system,
      functions.config().discord.activity,
  );
};

export const calcIndivisualSuperChats = async () => {
  const bot = initializeBot();
  try {
    const projectId = process.env.GCLOUD_PROJECT;
    const bigQuery = new BigQuery({projectId: projectId});

    // 実行タイミングの前日分までの月間スパチャ(個人のチャンネルごとのスパチャ金額)を集計
    // eslint-disable-next-line no-useless-escape
    const query = "SELECT process.supporterChannelId,sco.supporterDisplayName,sco.thumbnail,process.totalSuperChatAmount, process.channelId, process.channelTitle, sct.videoId    FROM (        SELECT sc.supporterChannelId,CONCAT(FORMAT('%\'.0f', sum(sc.amount)), '円') as totalSuperChatAmount, sc.channelId, channelTitle        FROM `discord-315419.channels.superChats` as sc        JOIN (            SELECT channelId,channelTitle FROM `discord-315419.channels.videos`            GROUP BY channelId,channelTitle) as channel        ON sc.channelId = channel.channelId        WHERE sc.paidAt < TIMESTAMP_SUB(TIMESTAMP(CURRENT_DATE('Asia/Tokyo')), INTERVAL 9 HOUR) AND sc.paidAt >= TIMESTAMP_SUB(TIMESTAMP(DATE_SUB(DATE_ADD(LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 DAY)), INTERVAL 1 DAY), INTERVAL 1 MONTH)), INTERVAL 9 HOUR)        GROUP BY supporterChannelId,channelId,sc.channelId,channelTitle        ) as process        JOIN (            select AS VALUE ARRAY_AGG(scb ORDER BY paidAt desc limit 1)[OFFSET(0)] from `discord-315419.channels.superChats` as scb where scb.paidAt < TIMESTAMP_SUB(TIMESTAMP(CURRENT_DATE('Asia/Tokyo')), INTERVAL 9 HOUR) AND scb.paidAt >= TIMESTAMP_SUB(TIMESTAMP(DATE_SUB(DATE_ADD(LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 DAY)), INTERVAL 1 DAY), INTERVAL 1 MONTH)), INTERVAL 9 HOUR) group by supporterChannelId         ) as sco        ON sco.supporterChannelId = process.supporterChannelId        JOIN (            select AS VALUE ARRAY_AGG(scb ORDER BY amount desc limit 1)[OFFSET(0)] from `discord-315419.channels.superChats` as scb where scb.paidAt < TIMESTAMP_SUB(TIMESTAMP(CURRENT_DATE('Asia/Tokyo')), INTERVAL 9 HOUR) AND scb.paidAt >= TIMESTAMP_SUB(TIMESTAMP(DATE_SUB(DATE_ADD(LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 DAY)), INTERVAL 1 DAY), INTERVAL 1 MONTH)), INTERVAL 9 HOUR) group by supporterChannelId         ) as sct         ON sct.supporterChannelId = process.supporterChannelId";
    const rows = await exec(bigQuery, query).catch((err) => {
      bot.alert(`${err.message}\n${err.stack}`);
      functions.logger.error(`${err.message}\n${err.stack}`);
    });
    if (!rows) {
      bot.alert("今日の個人スパチャ金額の集計は失敗に終わった");
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
      await upload(group[key], "monthly.tsv", `user/${key}/`);
    }
  } catch (err) {
    bot.alert(`${err.message}\n${err.stack}`);
    throw err;
  }
};

const getGroupId = (id: string) => {
  return id.slice(0, 3);
};

const rowToTsv = (row: any) => {
  return `${row.supporterChannelId}\t${row.supporterDisplayName}\t${row.thumbnail}\t${row.totalSuperChatAmount}\t${row.channelId}\t${row.channelTitle}\n`;
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

const sleep = (msec: number) => new Promise((resolve) => setTimeout(resolve, msec));
