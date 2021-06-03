import axios from "axios";

const VIDEO_ENDPOINT = "https://www.youtube.com/watch";
const CHAT_ENDPOINT = "https://www.youtube.com/youtubei/v1/live_chat/get_live_chat_replay";
const PARALLEL_CNUMBER = 10;

/**
 * チャットがサポートされていないアーカイブをクロールした場合に、パースエラー等と切り分けるために用意。
 */
export class ChatUnavailableError extends Error {}

export const findChatMessages = async (videoId: string, streamLengthSec: number) => {
  const chats = await fetchChatsParallel(videoId, streamLengthSec);

  const superchats = chats.superchats;
  const subscribes = chats.subscribes;

  let amount = 0;
  for (const key of Object.keys(superchats)) {
    amount += stringToAmount(superchats[key]);
  }

  const result = {
    chatCount: chats.chatCount,
    superChatCount: Object.keys(superchats).length,
    superChatAmount: amount,
    subscribeCount: Object.keys(subscribes).length,
  };

  return result;
};

const fetchChatsParallel = async (videoId: string, streamLengthSec: number) => {
  const timeUnit = Math.floor(streamLengthSec / PARALLEL_CNUMBER);
  const fetchVideoList = [];
  for (let i = 0; i < PARALLEL_CNUMBER; i++) {
    const time = timeUnit * i + "s";
    fetchVideoList.push(fetchVideoPage(videoId, time));
  }

  const result = await Promise.all(fetchVideoList) as Array<any>;

  const obj = result.reduce((result, response) => {
    result.push({
      apiKey: findKey("INNERTUBE_API_KEY", response.data),
      continuation: findContinuation("continuation", response.data),
      visitor: findKey("visitorData", response.data),
      client: findKey("clientVersion", response.data),
    });
    return result;
  }, []);

  const firstChatList = [];
  for (const data of obj) {
    firstChatList.push(fetchFirstChat(data));
  }
  const chatIds = await Promise.all(firstChatList);

  const fetchChatList = [];
  for (const [i, data] of obj.entries()) {
    const filterdIds = chatIds.filter((id, j) => i !== j); // 自分自身のIDをリストから削除
    const pickedIds = filterdIds.filter((id) => id !== chatIds[i]); // 自分自身のIDが他のIDと重複していた場合、残ったIDも消す。
    const uniqIds = [...new Set(pickedIds)]; // 最終的に残った中で重複IDがある場合はユニークにする
    fetchChatList.push(fetchChats(data, uniqIds));
  }
  const chats = await Promise.all(fetchChatList);

  return chats.reduce((total: any, chat: any) => {
    total.chatCount += chat.chatCount;
    total.superchats = {...total.superchats, ...chat.superchats};
    total.subscribes = {...total.subscribes, ...chat.subscribes};
    return total;
  }, {
    chatCount: 0,
    superchats: {},
    subscribes: {},
  });
};

const fetchFirstChat = async (data: any) => {
  const chatdataResponse = await fetchChatData(data.apiKey, data.continuation, data.visitor, data.client);
  const chatActions = chatdataResponse.data.continuationContents.liveChatContinuation.actions;
  if (chatActions && chatActions.length > 0) {
    const chats = processChats(chatActions, []);
    return chats.firstChatId;
  }
  return null;
};

const fetchChats = async (data: any, chatIds: Array<string|null>) => {
  let chatCount = 0;
  let superchats:any = {};
  let subscribes:any = {};

  let nextContinuation = data.continuation;
  for (;;) {
    const chatdataResponse = await fetchChatData(data.apiKey, nextContinuation, data.visitor, data.client);
    const chatActions = chatdataResponse.data.continuationContents.liveChatContinuation.actions;

    if (chatActions && chatActions.length > 0) {
      const result = processChats(chatActions, chatIds);
      chatCount += result.chatCount;
      superchats = {...superchats, ...result.superchats};
      subscribes = {...subscribes, ...result.subscribes};
      if (result.end) {
        break;
      }
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

  return {
    chatCount: chatCount,
    superchats: superchats,
    subscribes: subscribes,
  };
};

const fetchVideoPage = async (videoId: string, time: string) => {
  const params = {
    "v": videoId,
    "t": time,
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

const processChats = (chats: Array<any>, chatIds: Array<string|null>) => {
  let firstChatId:string|null = null;
  let chatCount = 0;
  const superchats:any = {};
  const subscribes:any = {};
  const end = chats.some((chat: any) => {
    return chat.replayChatItemAction.actions.some((action: any) => {
      if (action.addChatItemAction?.item?.liveChatPaidMessageRenderer) {
        superchats[action.addChatItemAction.item.liveChatPaidMessageRenderer.id] = action.addChatItemAction.item.liveChatPaidMessageRenderer.purchaseAmountText.simpleText;
      }
      if (action.addLiveChatTickerItemAction?.item?.liveChatTickerPaidMessageItemRenderer) {
        // スパチャの処理が上とかぶるのでこちらは無視
      }
      if (action.addChatItemAction?.item?.liveChatTextMessageRenderer) {
        const chatId = action.addChatItemAction.item.liveChatTextMessageRenderer.id;
        if (chatIds.includes(chatId)) {
          return true;
        }
        if (firstChatId === null) {
          firstChatId = chatId;
        }
        chatCount += 1;
      }
      if (action.addChatItemAction?.item?.liveChatMembershipItemRenderer) {
        subscribes[action.addChatItemAction.item.liveChatMembershipItemRenderer.id] = 1;
      }
      if (action.addChatItemAction?.item?.liveChatViewerEngagementMessageRenderer) {
        // metadata
      }
      if (action.addChatItemAction?.item?.liveChatPlaceholderItemRenderer) {
        // metadata
      }
      return false;
    });
  });

  return {
    chatCount: chatCount,
    superchats: superchats,
    subscribes: subscribes,
    firstChatId: firstChatId,
    end: end,
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
    case "SAR":
      return 29.21;
    case "AED":
      return 29.84;
    case "UYU":
      return 2.51;
    default:
      throw new Error("為替レートの処理中にエラーが発生しました:"+unit);
  }
};
