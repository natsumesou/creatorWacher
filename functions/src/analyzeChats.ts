import * as functions from "firebase-functions";
import {ChatNotFoundError, findChatMessages, SuperChat, VIDEO_ENDPOINT} from "./lib/youtubeChatFinder";
import {Bot} from "./lib/discordNotify";
import {DocumentSnapshot} from "firebase-functions/lib/providers/firestore";
import * as admin from "firebase-admin";
import {Message} from "firebase-functions/lib/providers/pubsub";

export const analyzeChats = async (message: Message) => {
  const metadata = messageToJSON(message);

  const db = admin.firestore();
  const streamRef = db.collection(`channels/${metadata.channelId}/streams`).doc(metadata.videoId);
  const stream = await streamRef.get().catch((err) => {
    functions.logger.error(err.message);
  });
  const channel = await streamRef.parent.parent?.get();
  if (!channel?.exists || !(stream && stream?.exists)) {
    throw new Error(`チャンネルか動画データがfirestoreから取得できません: ${JSON.stringify(metadata)}`);
  }
  const category = channel.get("category");
  const bot = new Bot(
      functions.config().discord[category],
      functions.config().discord.system,
      functions.config().discord.activity,
  );
  try {
    const result = await findChatMessages(stream.id, stream.get("streamLengthSec"));
    await updateStream(stream, result.stream);
    if (!result.stream.chatAvailable) {
      await bot.message(formatNonChatMessage(stream, result.stream));
    } else {
      await saveSuperChats(metadata, result.superChats);
      await bot.message(formatMessage(stream, result.stream));
    }
  } catch (err) {
    if (err instanceof ChatNotFoundError) {
      if (stream.get("chatAvailable") !== false || stream.get("chatAvailable") !== false) {
        await updateStream(stream, {chatAvailable: false, chatDisabled: false});
      }
      await processChatNotFound(bot, stream);
    } else {
      const message = err.message + "\n<" + generateURL(stream.id)+">\n" + err.stack;
      await bot.alert(message);
      throw new Error(message);
    }
  }
};

const messageToJSON = (message: Message) => {
  const jsonstr = Buffer.from(message.data, "base64").toString("utf-8");
  return JSON.parse(jsonstr) as {
    channelId: string,
    videoId: string,
  };
};

const updateStream = async (snapshot: DocumentSnapshot, data: any) => {
  await snapshot.ref.update({...data, updatedAt: new Date()}).catch((err) => {
    functions.logger.error(err.message);
  });
};

const saveSuperChats = async (metadata: any, superChats: {[id:string]: SuperChat}) => {
  const db = admin.firestore();
  let batch = db.batch();
  const limit = 500;
  let i = 0;
  /* eslint-disable guard-for-in */
  for (const id in superChats) {
    if (!Object.prototype.hasOwnProperty.call(superChats, id)) {
      console.log("!! continue?: " + id);
      console.log(JSON.stringify(superChats));
      continue;
    }
    console.log("!! superchat: " + id);
    const doc = db.collection(`channels/${metadata.channelId}/streams/${metadata.videoId}/superChats`).doc(id);
    batch.set(doc, superChats[id]);
    i += 1;
    if (i === limit) {
      await batch.commit().catch((err) => {
        functions.logger.error(err.message);
      });
      batch = db.batch();
      i = 0;
    }
  }
  if (i !== limit) {
    console.log("!! commit: " + i);
    await batch.commit().catch((err) => {
      functions.logger.error(err.message);
    });
  }
};

const processChatNotFound = async (bot: Bot, snapshot: DocumentSnapshot) => {
  const now = new Date();
  const publishedAt = snapshot.get("publishedAt").toDate();

  if (passedDays(now, publishedAt) < 7) {
    // 配信後4回目のクローリングのときだけメッセージを流す
    // 初回のクローリング時点ではチャットが取得できないことが多いのでスルー
    if (passedHours(now, publishedAt) > 1.5 && passedHours(now, publishedAt) < 2) {
      const message = "チャットがオフになっている(もしくはYouTubeの仕様が変わった)可能性が高いため１日監視します。頻発する場合は仕様の再確認をしてください。\n" + generateURL(snapshot.id);
      await bot.activity(message);
    }
  } else {
    const message = "チャットが戻らないまま7日経ったので監視を終了します\n" + generateURL(snapshot.id);
    await Promise.all([
      bot.message(formatNonChatMessage(snapshot)),
      bot.activity(message),
    ]);
  }
};

const passedHours = (now: Date, publishedAt: Date) => {
  const millisecondsPerHour = 1000 * 60 * 60;
  return getDateDiff(now, publishedAt, millisecondsPerHour);
};

const passedDays = (now: Date, publishedAt: Date) => {
  const millisecondsPerDay = 1000 * 60 * 60 * 24;
  return getDateDiff(now, publishedAt, millisecondsPerDay);
};

const getDateDiff = (now: Date, old: Date, unitMillisec: number) => {
  const millisBetween = now.getTime() - old.getTime();
  return Math.abs(millisBetween / unitMillisec);
};

const formatMessage = (snapshot: DocumentSnapshot, chats: any) => {
  return formatMessageBase(snapshot) +
    "\nコメント数: " + threeDigit(chats.chatCount) +
    "\nスパチャ数: " + threeDigit(chats.superChatCount) +
    "\nスパチャ額: " + threeDigit(Math.round(chats.superChatAmount)) + "円" +
    "\nメンバー入会数: " + threeDigit(chats.subscribeCount) +
    "\n" + generateURL(snapshot.id);
};

const formatNonChatMessage = (snapshot: DocumentSnapshot, chats?: any) => {
  const status = (!chats || chats?.chatAvailable) ? "" : "[確定値]";
  return formatMessageBase(snapshot) +
    "\nチャットがオフのため詳細データなし" + status +
    "\n" + generateURL(snapshot.id);
};

const formatMessageBase = (snapshot: DocumentSnapshot) => {
  return snapshot.get("title") +
  "\n視聴数: " + threeDigit(snapshot.get("viewCount"));
};

const generateURL = (videoId: string) => {
  return VIDEO_ENDPOINT + "?v=" + videoId;
};

const threeDigit = (num: number) => {
  return String(num).replace( /(\d)(?=(\d\d\d)+(?!\d))/g, "$1,");
};
