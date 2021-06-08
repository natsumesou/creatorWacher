import * as functions from "firebase-functions";
import {ChatNotFoundError, findChatMessages, VIDEO_ENDPOINT} from "./lib/youtubeChatFinder";
import {Bot} from "./lib/discordNotify";
import {QueryDocumentSnapshot} from "firebase-functions/lib/providers/firestore";
import {EventContext} from "firebase-functions";

export const analyzeChats = async (snapshot: QueryDocumentSnapshot, context: EventContext) => {
  const doc = await snapshot.ref.parent.parent?.get();
  const category = doc?.get("category");
  const bot = new Bot(
      functions.config().discord[category],
      functions.config().discord.system,
      functions.config().discord.activity,
  );
  try {
    const chats = await findChatMessages(snapshot.id, snapshot.get("streamLengthSec"));
    if (!chats.chatAvailable) {
      await updateStream(snapshot, chats);
      await bot.message(formatNonChatMessage(snapshot, chats));
      return;
    }
    await updateStream(snapshot, chats);
    await bot.message(formatMessage(snapshot, chats));
  } catch (err) {
    if (err instanceof ChatNotFoundError) {
      await processChatNotFound(bot, snapshot);
    } else {
      const message = err.message + "\n<" + generateURL(snapshot.id)+">";
      await bot.alert(message);
      await deleteStream(snapshot);
      throw new Error(message);
    }
  }
};

const updateStream = async (snapshot: QueryDocumentSnapshot, data: any) => {
  await snapshot.ref.update(data).catch((err) => {
    functions.logger.error(err.message);
  });
};

const deleteStream = async (snapshot: QueryDocumentSnapshot) => {
  await snapshot.ref.delete().catch((err) => {
    functions.logger.error(err.message);
  });
};

const processChatNotFound = async (bot: Bot, snapshot: QueryDocumentSnapshot) => {
  const now = new Date();
  const publishedAt = snapshot.get("publishedAt").toDate();

  if (passedDays(now, publishedAt) < 1) {
    // 公開されて1日以内の場合はチャットが戻ってくる可能性があるので一度削除する
    await deleteStream(snapshot);

    // 配信後2回目のクローリングのときだけメッセージを流す
    // 初回のクローリング時点ではチャットが取得できないことが多いのでスルー
    if (passedHours(now, publishedAt) > 0.5 && passedHours(now, publishedAt) < 1) {
      const message = "チャットがオフになっている(もしくはYouTubeの仕様が変わった)可能性が高いため１日監視します。頻発する場合は仕様の再確認をしてください。\n" + generateURL(snapshot.id);
      await Promise.all([
        bot.message(formatNonChatMessage(snapshot)),
        bot.activity(message),
      ]);
      functions.logger.warn(message);
    }
  } else {
    const message = "チャットが戻らないまま1日経ったので監視を終了します\n" + generateURL(snapshot.id);
    await bot.activity(message);
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

const formatMessage = (snapshot: QueryDocumentSnapshot, chats: any) => {
  return formatMessageBase(snapshot) +
    "\nコメント数: " + threeDigit(chats.chatCount) +
    "\nスパチャ数: " + threeDigit(chats.superChatCount) +
    "\nスパチャ額: " + threeDigit(Math.round(chats.superChatAmount)) + "円" +
    "\nメンバー入会数: " + threeDigit(chats.subscribeCount) +
    "\n" + generateURL(snapshot.id);
};

const formatNonChatMessage = (snapshot: QueryDocumentSnapshot, chats?: any) => {
  const status = chats?.chatUnavailable ? "[確定値]" : "[速報値]";
  return formatMessageBase(snapshot) +
    "\nチャットがオフのため詳細データなし" + status +
    "\n" + generateURL(snapshot.id);
};

const formatMessageBase = (snapshot: QueryDocumentSnapshot) => {
  return snapshot.get("title") +
  "\n視聴数: " + threeDigit(snapshot.get("viewCount"));
};

const generateURL = (videoId: string) => {
  return VIDEO_ENDPOINT + "?v=" + videoId;
};

const threeDigit = (num: number) => {
  return String(num).replace( /(\d)(?=(\d\d\d)+(?!\d))/g, "$1,");
};
