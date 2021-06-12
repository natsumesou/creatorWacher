import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import {Message} from "firebase-functions/lib/providers/pubsub";
import {findArchivedStreams, CHANNEL_ENDPOINT, ChannelNotExistError, InvalidChannelJsonError} from "./lib/youtubeArchiveFinder";
import {Bot} from "./lib/discordNotify";
import {PubSub} from "@google-cloud/pubsub";
import {ANALYZE_TOPIC} from ".";

export const updateArchives = async (message: Message) => {
  const channel = messageToJSON(message);
  const bot = new Bot(
      functions.config().discord[channel.category],
      functions.config().discord.system,
      functions.config().discord.activity,
  );
  try {
    const now = new Date();
    const result = await findArchivedStreams(channel.id);
    await saveStream(channel, result.streams);
    if (firstTimeToday(now)) {
      await updateChannel(channel, result.subscribeCount);
    }
  } catch (err) {
    if (err instanceof ChannelNotExistError) {
      const message = err.message + "\n<" + CHANNEL_ENDPOINT + channel.id + ">";
      await bot.activity(message);
    } else if (err instanceof InvalidChannelJsonError) {
      // たまにチャンネル動画ページのJSONが空になる事があるので無視する
      // 万が一これで本当のエラーを握りつぶしている可能性がないように一応厳密にチェックはしている…
    } else {
      const message = err.message + "\n<" + CHANNEL_ENDPOINT + channel.id + ">\n" + err.stack;
      await bot.alert(message);
      throw new Error(message);
    }
  }
};

const messageToJSON = (message: Message) => {
  const jsonstr = Buffer.from(message.data, "base64").toString("utf-8");
  return JSON.parse(jsonstr);
};

const updateChannel = async (channel: any, subscribeCount: number|null) => {
  const db = admin.firestore();
  const channelRef = db.collection("channels").doc(channel.id);
  await channelRef.update({
    subscribeCount: subscribeCount,
  }).catch((err) => {
    functions.logger.error(err.message);
  });
};

const saveStream = async (channel: any, streams: Array<any>) => {
  const db = admin.firestore();
  for (const stream of streams) {
    const streamRef = db.collection(`channels/${channel.id}/streams`).doc(stream.id);
    const doc = await streamRef.get().catch((err) => {
      functions.logger.error(err.message);
    });

    if (doc && doc.exists) {
      if (doc.get("chatAvailable") === null || doc.get("chatDisabled") === null || (doc.get("chatAvailable") === false && doc.get("chatDisabled") === false)) {
        // チャットの取得状態が不明、若しくは前回の更新時にチャットの取得に失敗している場合は再チェックを促す
        await publishAnalyzeStream(doc.id, channel.id);
      }
      continue;
    }
    const videoId = stream.id;
    delete stream.id;
    await streamRef.create({...stream,
      chatAvailable: null, // チャットが取得できている場合はtrue、そうでない場合はfalse
      chatDisabled: null, // チャットが無効にされいてる場合はtrue、無効にされているかわからないけど取得できない場合含めその他はfalse
      gameTitle: null,
      chatCount: 0,
      superChatCount: 0,
      superChatAmount: 0,
      subscribeCount: 0,
    }).catch((err) => {
      functions.logger.error(err.message);
    });
    await publishAnalyzeStream(videoId, channel.id);
  }
};

const firstTimeToday = (now: Date) => {
  // hourはUTCを考慮して0ではなく15にしてる
  return now.getHours() === 15 && (now.getMinutes() >= 0 && now.getMinutes() < 30);
};

const publishAnalyzeStream = async (videoId: string, channelId: string) => {
  const pubsub = new PubSub({projectId: process.env.GCP_PROJECT});
  const topic = await pubsub.topic(ANALYZE_TOPIC);
  const obj = {
    videoId: videoId,
    channelId: channelId,
  };
  topic.publish(Buffer.from(JSON.stringify(obj)));
};
