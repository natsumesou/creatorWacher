import axios from "axios";

const VIDEO_ENDPOINT = "https://www.youtube.com/watch";
const CHAT_ENDPOINT = "https://www.youtube.com/youtubei/v1/live_chat/get_live_chat_replay";

/**
 * チャットがサポートされていないアーカイブをクロールした場合に、パースエラー等と切り分けるために用意。
 */
export class ChatUnavailableError extends Error {}

export const findChatMessages = async (videoId: string) => {
  const response = await fetchVideoPage(videoId);

  const apiKey = findKey("INNERTUBE_API_KEY", response.data);
  const continuation = findContinuation("continuation", response.data);
  const visitor = findKey("visitorData", response.data);
  const client = findKey("clientVersion", response.data);

  const chats = [];
  let nextContinuation = continuation;

  for (;;) {
    const chatdataResponse = await fetchChatData(apiKey, nextContinuation, visitor, client);
    const chatActions = chatdataResponse.data.continuationContents.liveChatContinuation.actions;
    if (chatActions && chatActions.length > 0) {
      chats.push(chatActions);
    }
    const nextCont = chatdataResponse.data.continuationContents.liveChatContinuation.continuations.find((cont: any) => {
      return cont.liveChatReplayContinuationData !== undefined;
    });
    if (nextCont) {
      nextContinuation = nextCont.liveChatReplayContinuationData.continuation;
    } else {
      break;
    }
  }

  try {
    const result = processChats(chats.flat());
    const chatCount = result.chatCount;
    const superChatCount = result.superChatCount;
    const superChatAmount = result.superChatAmount;
    const subscribeCount = result.subscribeCount;

    return {
      chatCount: chatCount,
      superChatCount: superChatCount,
      superChatAmount: superChatAmount,
      subscribeCount: subscribeCount,
    };
  } catch (err) {
    throw new Error(videoId + ":" + err);
  }
};

const fetchVideoPage = async (videoId: string) => {
  const params = {
    "v": videoId,
  };
  const headers = {
    "Cookie": "PREF=\"tz=Asia.Tokyo&hl=ja\";",
  };
  return await axios.get(VIDEO_ENDPOINT, {
    params: params,
    headers: headers,
  });
};

const fetchChatData = async (apiKey: string, continuation: string, visitor: string, client: string) => {
  const userAgent = "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36";
  const data = {
    context: {
      client: {
        clientName: "WEB",
        clientVersion: client,
        visitorData: visitor,
        userAgent,
      },
    },
    continuation: continuation,
  };
  const headers = {
    "user-agent": userAgent,
  };
  const response = await axios.post(CHAT_ENDPOINT, data, {
    headers: headers,
    params: {"key": apiKey},
  });
  return response;
};

const findContinuation = (keyName: string, sourcee: string) => {
  try {
    return findVariable(keyName, sourcee, 100); // continuationが複数あり、小さい値の方は偽物なので弾く。
  } catch (error) {
    throw new ChatUnavailableError("チャットがオフになっています");
  }
};

const findKey = (keyName: string, source: string) => {
  return findVariable(keyName, source, 1);
};

const findVariable = (keyName: string, source: string, minChar: number) => {
  const re = new RegExp("\"" + keyName + "\":\"([^\"]{"+minChar+",})\"");
  const match = re.exec(source);
  if (match === null) {
    throw new Error("動画ページ内の<" + keyName + ">が見つかりません。");
  }
  return match[1];
};

const processChats = (chats: Array<any>) => {
  let superChatCount = 0;
  let superChatAmount = 0;
  let chatCount = 0;
  let subscribeCount = 0;
  chats.forEach((chat: any) => {
    let c = 0;
    chat.replayChatItemAction.actions.forEach((action: any) => {
      if (c > 1) {
        console.log("複数アクションあり:"+JSON.stringify(action));
      }
      c += 1;
      if (action.addChatItemAction?.item?.liveChatPaidMessageRenderer !== undefined) {
        // スパチャの処理が下とかぶるのでこちらは無視
      }
      if (action.addLiveChatTickerItemAction?.item?.liveChatTickerPaidMessageItemRenderer !== undefined) {
        superChatCount += 1;
        const amountText = action.addLiveChatTickerItemAction.item.liveChatTickerPaidMessageItemRenderer.amount.simpleText;
        superChatAmount += stringToAmount(amountText);
      }
      if (action.addChatItemAction?.item?.liveChatTextMessageRenderer !== undefined) {
        chatCount += 1;
      }
      if (action.addChatItemAction?.item?.liveChatMembershipItemRenderer !== undefined) {
        subscribeCount += 1;
      }
      if (action.addChatItemAction?.item?.liveChatViewerEngagementMessageRenderer !== undefined ||
        action.addChatItemAction?.item?.liveChatPlaceholderItemRenderer !== undefined) {
        // NOTHING TO DO
      }
    });
  });

  return {
    chatCount: chatCount,
    superChatCount: superChatCount,
    superChatAmount: superChatAmount,
    subscribeCount: subscribeCount,
  };
};

const stringToAmount = (str: string) => {
  const match = str.match(/([^0-9]+)([0-9,.]+)/);
  if (match === null) {
    throw new Error("通貨の処理中にエラーが発生しました:" + str);
  }
  const price = parseFloat(match[2].replace(",", ""));
  return price * rate(match[1]);
};

const rate = (unit: string) => {
  switch (unit.trim()) {
    case "$":
      return 110.0;
    case "A$":
      return 73.67;
    case "CA$":
      return 77;
    case "CHF":
      return 113.0;
    case "COP":
      return 0.03;
    case "HK$":
      return 13.8;
    case "HUF":
      return 0.34;
    case "MX$":
      return 4.72;
    case "NT$":
      return 3;
    case "NZ$":
      return 68.86;
    case "PHP":
      return 2.14;
    case "PLN":
      return 27.01;
    case "R$":
      return 20.14;
    case "RUB":
      return 1.5;
    case "SEK":
      return 11.48;
    case "£":
      return 135.0;
    case "₩":
      return 0.1;
    case "€":
      return 120;
    case "₹":
      return 1.42;
    case "¥":
      return 1;
    case "PEN":
      return 30.56;
    case "ARS":
      return 1.53;
    case "CLP":
      return 0.13;
    case "NOK":
      return 11.08;
    case "BAM":
      return 61.44;
    case "SGD":
      return 77.02;
    case "CZK":
      return 4.49;
    case "ZAR":
      return 6.05;
    case "RON":
      return 25.91;
    case "BYN":
      return 43.16;
    case "₱":
      return 2.14;
    case "MYR":
      return 26.56;
    case "₪":
      return 33.6765;
    case "DKK":
      return 17.99;
    case "CRC":
      return 0.18;
    default:
      throw new Error("為替レートの処理中にエラーが発生しました:"+unit);
  }
};
