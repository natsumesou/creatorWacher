import axios from "axios";
import {ExchangeRateManager} from "./exchangeRateManager";

export const VIDEO_ENDPOINT = "https://www.youtube.com/watch";
const CHAT_ENDPOINT = "https://www.youtube.com/youtubei/v1/live_chat/get_live_chat_replay";
const PARALLEL_CNUMBER = 10;

/**
 * チャットデータが取得できなかった場合のエラー
 */
export class ChatNotFoundError extends Error {}

/**
 * 配信のチャットログをパースしてスパチャやメンバー加入数を分析する
 * returnで返すsubscribeCountはチャンネル登録者数ではなく配信でのメンバー加入数のこと
 * @param {string} videoId video id
 * @param {number} streamLengthSec video length
 * @return {any} analyzed data
 */
export const findChatMessages = async (videoId: string, streamLengthSec: number) => {
  const chats = await fetchChatsParallel(videoId, streamLengthSec);
  if (chats.chatUnavailable) {
    return {
      chatAvailable: !chats.chatUnavailable,
      gameTitle: chats.gameTitle,
      chatCount: 0,
      superChatCount: 0,
      superChatAmount: 0,
      subscribeCount: 0,
    };
  }

  const superchats = chats.superchats;
  const subscribes = chats.subscribes;

  let amount = 0;
  const rate = new ExchangeRateManager();
  for (const key of Object.keys(superchats)) {
    amount += stringToAmount(rate, superchats[key]);
  }

  const result = {
    chatAvailable: true,
    gameTitle: chats.gameTitle,
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
  const responses = await Promise.all(fetchVideoList) as Array<any>;

  const json = getInitialJSON(responses[0].data);
  const gameTitle = parseJSONtoFindGameTitle(json);

  const params = [];
  for (const response of responses) {
    const param = getChatRequestParams(response.data, getInitialJSON(response.data));
    if (param) {
      params.push(param);
    }
  }
  if (params.length !== responses.length) {
    return {
      gameTitle: gameTitle,
      chatUnavailable: true,
    };
  }

  const firstChatList = [];
  for (const data of params) {
    firstChatList.push(fetchChat(data, []));
  }
  const firstChatBlock = await Promise.all(firstChatList);
  const chatIds = firstChatBlock.map((chats) => chats.firstChatId);

  const fetchChatList = [];
  for (const [i, data] of params.entries()) {
    const continuation = firstChatBlock[i].nextContinuation;
    if (continuation === null) {
      continue;
    }
    data.continuation = continuation;
    const uniqIds = [...new Set(chatIds)]; // 重複IDがある場合はユニークにする
    fetchChatList.push(fetchChats(data, uniqIds));
  }
  const chats = await Promise.all(fetchChatList);

  const result = chats.concat(firstChatBlock).reduce((total: any, chat: any) => {
    total.chatCount += chat.chatCount;
    total.superchats = {...total.superchats, ...chat.superchats};
    total.subscribes = {...total.subscribes, ...chat.subscribes};
    return total;
  }, {
    gameTitle: gameTitle,
    chatCount: 0,
    superchats: {},
    subscribes: {},
  });

  return result;
};

const getChatRequestParams = (html: string, json: any) => {
  if (chatUnavailable(html)) {
    return null;
  }
  const apiKey = findKey("INNERTUBE_API_KEY", html);
  const continuation = findContinuation(json);
  const clientVersion = findKey("clientVersion", html);
  if (!apiKey || !continuation || !clientVersion) {
    throw new ChatNotFoundError(`
      チャットが取得できません(
      apiKey: ${apiKey},
      continuation: ${continuation},
      clientVersion: ${clientVersion}
    )`);
  }
  return {
    apiKey: apiKey,
    continuation: continuation,
    client: clientVersion,
  };
};

const getInitialJSON = (html: string) => {
  const match = html.match(/var ytInitialData = (.+);<\/script>/);
  if (match === null) {
    return null;
  }
  return JSON.parse(match[1]);
};

const parseJSONtoFindGameTitle = (json: any) => {
  const contents = json.contents.twoColumnWatchNextResults.results.results.contents;
  const streamMeta = contents.find((content:any) => {
    return content.videoSecondaryInfoRenderer !== undefined;
  });
  // 動画の音楽情報が登録されている場合に当てはまってしまう
  // 動画とゲーム両方登録されていた場合の挙動は未検証
  if (streamMeta.videoSecondaryInfoRenderer.metadataRowContainer.metadataRowContainerRenderer.rows &&
    streamMeta.videoSecondaryInfoRenderer.metadataRowContainer.metadataRowContainerRenderer.rows[0].richMetadataRowRenderer
  ) {
    return streamMeta.videoSecondaryInfoRenderer.metadataRowContainer.metadataRowContainerRenderer.rows[0].richMetadataRowRenderer.contents[0].richMetadataRenderer.title?.simpleText || null;
  } else {
    return null;
  }
};

const fetchChats = async (data: any, chatIds: Array<string|null>) => {
  let chatCount = 0;
  let superchats:any = {};
  let subscribes:any = {};

  for (;;) {
    const chats = await fetchChat(data, chatIds);
    chatCount += chats.chatCount;
    superchats = {...superchats, ...chats.superchats};
    subscribes = {...subscribes, ...chats.subscribes};

    if (chats.nextContinuation) {
      data.continuation = chats.nextContinuation;
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

const fetchChat = async (data: any, chatIds: Array<string|null>) => {
  const chatdataResponse = await fetchChatData(data.apiKey, data.continuation, data.client);
  const chatActions = chatdataResponse.data.continuationContents.liveChatContinuation.actions;

  const chats: {
    chatCount: number,
    superchats: any,
    subscribes: any,
    firstChatId: string|null,
    nextContinuation: string|null,
  } = {
    chatCount: 0,
    superchats: {},
    subscribes: {},
    firstChatId: null,
    nextContinuation: null,
  };

  if (chatActions === undefined || chatActions.length === 0) {
    return chats;
  }
  const result = processChats(chatActions, chatIds);
  const nextCont = chatdataResponse.data.continuationContents.liveChatContinuation.continuations.find((cont: any) => {
    return cont.liveChatReplayContinuationData !== undefined;
  });
  chats.chatCount = result.chatCount;
  chats.superchats = result.superchats;
  chats.subscribes = result.subscribes;
  chats.firstChatId = result.firstChatId;
  // 次のチャットを読み込んでも良い場合のみnextContinuationを代入する
  if (nextCont && !result.end) {
    chats.nextContinuation = nextCont.liveChatReplayContinuationData.continuation;
  }
  return chats;
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

const fetchChatData = async (apiKey: string, continuation: string, client: string) => {
  const userAgent = "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36";
  const data = {
    context: {
      client: {
        clientName: "WEB",
        clientVersion: client,
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

const chatUnavailable = (source: string) => {
  return source.match(/この動画ではチャットのリプレイを利用できません|この動画のチャットのリプレイはオフになっています/) !== null;
};

const findContinuation = (json: any) => {
  if (!json.contents.twoColumnWatchNextResults.conversationBar) {
    return null;
  }
  return json.contents.twoColumnWatchNextResults.conversationBar.liveChatRenderer.continuations[0].reloadContinuationData.continuation;
};

const findKey = (keyName: string, source: string) => {
  return findVariable(keyName, source, 1);
};

const findVariable = (keyName: string, source: string, minChar: number) => {
  const re = new RegExp("\"" + keyName + "\":\"([^\"]{"+minChar+",})\"");
  const match = re.exec(source);
  if (match === null) {
    return null;
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
        // 重複が見つかった時点で集計処理を止める
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

const stringToAmount = (rate: ExchangeRateManager, str: string) => {
  const match = str.match(/([^0-9]+)([0-9,.]+)/);
  if (match === null) {
    throw new Error("通貨の処理中にエラーが発生しました:" + str);
  }
  const price = parseFloat(match[2].replace(",", ""));
  return price * rate.getCurrentRate(match[1].trim());
};
