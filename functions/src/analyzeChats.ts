import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import {findChatMessages, ChatUnavailableError, ChatNotFoundError} from "./lib/youtubeChatFinder";
import {Bot} from "./lib/discordNotify";
import {QueryDocumentSnapshot} from "firebase-functions/lib/providers/firestore";
import {EventContext} from "firebase-functions";

export const analyzeChats = async (snapshot: QueryDocumentSnapshot, context: EventContext) => {
  const bot = new Bot(
      functions.config().discord.general,
      functions.config().discord.system,
      functions.config().discord.activity,
  );
  try {
    const chats = await findChatMessages(snapshot.id, snapshot.get("streamLengthSec"));
    await updateStream(snapshot.id, chats);
    await bot.message(formatMessage(snapshot, chats));
  } catch (err) {
    if (err instanceof ChatUnavailableError) {
      await updateStream(snapshot.id, {chatDisabled: true});
      await bot.message(formatNonChatMessage(snapshot, true));
    } else if (err instanceof ChatNotFoundError) {
      const now = new Date();
      const publishedAt = snapshot.get("publishedAt").toDate();
      if (withinAday(now, publishedAt)) {
        // 公開されて1日以内の場合はチャットが戻ってくる可能性があるので一度削除する
        await deleteStream(snapshot);
        
        // 配信後最初のクローリングのときだけメッセージを流す
        if (withinHalfAnHour(now, publishedAt)) {
          const message = "チャットがオフになっている(もしくはYouTubeの仕様が変わった)可能性が高いため１日監視します。頻発する場合は仕様の再確認をしてください。\n" + generateURL(snapshot.id);
          await Promise.all([
            bot.message(formatNonChatMessage(snapshot, false)),
            bot.activity(message),
          ]);
          functions.logger.warn(message);
        }
      } else {
        const message = "チャットが戻らないまま1日経ったので監視を終了します\n" + generateURL(snapshot.id);
        await bot.activity(message);
      }
    } else {
      const message = err.message + "\n" + generateURL(snapshot.id);
      await bot.alert(message);
      await deleteStream(snapshot);
      throw new Error(message);
    }
  }
};

const updateStream = async (videoId: string, data: any) => {
  const db = admin.firestore();
  await db.collection("Stream").doc(videoId).update(data).catch((err) => {
    functions.logger.error(err.message);
  });
};

const deleteStream = async (snapshot: QueryDocumentSnapshot) => {
  const db = admin.firestore();
  await db.collection("Stream").doc(snapshot.id).delete().catch((err) => {
    functions.logger.error(err.message);
  });
};

const withinHalfAnHour = (now: Date, publishedAt: Date) => {
  const millisecondsPerDay = 1000 * 60;
  const millisBetween = now.getTime() - publishedAt.getTime();
  const minutes = millisBetween / millisecondsPerDay;
  return minutes < 30;
};

const withinAday = (now: Date, publishedAt: Date) => {
  const millisecondsPerDay = 1000 * 60 * 60 * 24;
  const millisBetween = now.getTime() - publishedAt.getTime();
  const days = millisBetween / millisecondsPerDay;
  return days < 1;
};

const formatMessage = (snapshot: QueryDocumentSnapshot, chats: any) => {
  return formatMessageBase(snapshot) +
    "\nコメント数: " + threeDigit(chats.chatCount) +
    "\nスパチャ数: " + threeDigit(chats.superChatCount) +
    "\nスパチャ額: " + threeDigit(Math.round(chats.superChatAmount)) + "円" +
    "\nメンバー入会数: " + threeDigit(chats.subscribeCount) +
    "\n" + generateURL(snapshot.id);
};

const formatNonChatMessage = (snapshot: QueryDocumentSnapshot, chatDisabled: boolean) => {
  const status = chatDisabled ? "[確定値]" : "[速報値]";
  return formatMessageBase(snapshot) +
    "\nチャットがオフのため詳細データなし" + status +
    "\n" + generateURL(snapshot.id);
};

const formatMessageBase = (snapshot: QueryDocumentSnapshot) => {
  return snapshot.get("title") +
    "\n視聴数: " + threeDigit(snapshot.get("viewCount"));
};

const generateURL = (videoId: string) => {
  return "https://www.youtube.com/watch?v=" + videoId;
};

const threeDigit = (num: number) => {
  return String(num).replace( /(\d)(?=(\d\d\d)+(?!\d))/g, "$1,");
};
