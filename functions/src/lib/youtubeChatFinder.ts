import axios from "axios";
import {ExchangeRateManager} from "./exchangeRateManager";
import {upload} from "./cloudStorage";

export const VIDEO_ENDPOINT = "https://www.youtube.com/watch";
const CHAT_ENDPOINT = "https://www.youtube.com/youtubei/v1/live_chat/get_live_chat_replay";

export type SuperChat = {
  supporterChannelId: string,
  supporterDisplayName: string,
  paidAt: Date,
  amount: number,
  unit: string,
  amountText: string,
  thumbnail: string,
  message: string,
};

/**
 * チャットデータが取得できなかった場合のエラー
 */
export class ChatNotFoundError extends Error {}

/**
 * 配信のチャットログをパースしてスパチャやメンバー加入数を分析する
 * returnで返すsubscribeCountはチャンネル登録者数ではなく配信でのメンバー加入数のこと
 * @param {string} videoId video id
 * @param {number} streamLengthSec video length
 * @param {number} concurrency concurrency number to fetch chat data
 * @return {any} analyzed data
 */
export const findChatMessages = async (videoId: string, streamLengthSec: number, concurrency = 10) => {
  const chats = await fetchChatsParallel(videoId, streamLengthSec, concurrency);
  console.log("done");
  if (!chats.chatAvailable) {
    const result = {
      stream: {
        chatAvailable: chats.chatAvailable,
        chatDisabled: chats.chatDisabled,
        gameTitle: chats.gameTitle,
        chatCount: 0,
        superChatCount: 0,
        superChatAmount: 0,
        subscribeCount: 0,
      },
      superChats: {},
    };
    return result;
  }

  const superchats = chats.superchats;
  const subscribes = chats.subscribes;

  let amount = 0;
  for (const key of Object.keys(superchats)) {
    amount += superchats[key].amount;
  }

  const result = {
    stream: {
      chatAvailable: chats.chatAvailable,
      chatDisabled: chats.chatDisabled,
      gameTitle: chats.gameTitle,
      chatCount: chats.chatCount,
      superChatCount: Object.keys(superchats).length,
      superChatAmount: amount,
      subscribeCount: Object.keys(subscribes).length,
    },
    superChats: superchats,
  };
  return result;
};

const fetchChatsParallel = async (videoId: string, streamLengthSec: number, concurrency: number) => {
  const timeUnit = Math.floor(streamLengthSec / concurrency);
  const fetchVideoList = [];
  for (let i = 0; i < concurrency; i++) {
    const time = timeUnit * i + "s";
    fetchVideoList.push(fetchVideoPage(videoId, time));
  }
  const responses = await Promise.all(fetchVideoList) as Array<any>;

  const json = getInitialJSON(responses[0].data);
  const gameTitle = parseJSONtoFindGameTitle(json);
  await temp(videoId, json);

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
      chatAvailable: false,
      chatDisabled: true,
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
    chatAvailable: true,
    chatDisabled: false,
    gameTitle: gameTitle,
    chatCount: 0,
    superchats: {},
    subscribes: {},
  });

  return result;
};

const getChatRequestParams = (html: string, json: any) => {
  if (chatDisabled(html)) {
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
  const match = html.match(/var ytInitialData = (.+});<\/script>/);
  if (match === null) {
    return null;
  }
  return JSON.parse(match[1]);
};

const temp = async (videoId: string, json: any) => {
  try {
    const desc = json.contents?.twoColumnWatchNextResults?.results?.results?.contents[1]?.videoSecondaryInfoRenderer?.description?.runs;
    const d = desc.reduce((r: string, d: {[text: string]: string}) => {
      if (r.length + d.text.length <= 500) {
        r += d.text;
      }
      return r;
    }, "");
    await upload(d, `${videoId}.tsv`, "tmp/");
  } catch (err) {
    console.error("書き捨てエラー: " + err.message);
  }
};

const parseJSONtoFindGameTitle = (json: any) => {
  const contents = json.contents.twoColumnWatchNextResults.results.results.contents;
  const streamMeta = contents.find((content:any) => {
    return content !== undefined && content?.videoSecondaryInfoRenderer !== undefined;
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
  console.log("fetch continuation: " + data.continuation);
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

const chatDisabled = (source: string) => {
  return source.match(/この動画ではチャットのリプレイを利用できません|この動画のチャットのリプレイはオフになっています/) !== null;
};

const findContinuation = (json: any) => {
  if (!json.contents.twoColumnWatchNextResults.conversationBar?.liveChatRenderer) {
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
  const rate = new ExchangeRateManager();

  let firstChatId:string|null = null;
  let chatCount = 0;
  const superchats:any = {};
  const subscribes:any = {};
  const end = chats.some((chat: any) => {
    return chat.replayChatItemAction.actions.some((action: any) => {
      if (action.addChatItemAction?.item?.liveChatPaidMessageRenderer || action.addChatItemAction?.item?.liveChatPaidStickerRenderer) {
        const renderer = action.addChatItemAction?.item?.liveChatPaidMessageRenderer || action.addChatItemAction?.item?.liveChatPaidStickerRenderer;
        const isSticker = !!action.addChatItemAction?.item?.liveChatPaidStickerRenderer;
        const amountText = renderer.purchaseAmountText.simpleText;
        const amountinfo = stringToAmount(rate, amountText);
        const id = renderer.id;
        const message = isSticker ? null : renderer.message ? renderer.message.runs[0].text : "";

        if (!superchats[id]) {
          superchats[id] = {} as SuperChat;
        }
        const meta = {
          supporterChannelId: renderer.authorExternalChannelId,
          supporterDisplayName: renderer.authorName.simpleText,
          paidAt: new Date(parseInt(renderer.timestampUsec.slice(0, -3))),
          amount: amountinfo.amount,
          unit: amountinfo.unit,
          amountText: amountText,
          message: message,
        };
        superchats[id] = {...superchats[id], ...meta};
      }
      if (action.addLiveChatTickerItemAction?.item?.liveChatTickerPaidMessageItemRenderer || action.addLiveChatTickerItemAction?.item?.liveChatTickerPaidStickerItemRenderer) {
        const renderer = action.addLiveChatTickerItemAction?.item?.liveChatTickerPaidMessageItemRenderer || action.addLiveChatTickerItemAction?.item?.liveChatTickerPaidStickerItemRenderer;
        const id = renderer.id;
        if (!superchats[id]) {
          superchats[id] = {} as SuperChat;
        }

        let biggerIndex = 0;
        let biggerWidth = 0;
        renderer.authorPhoto.thumbnails.map((thumb: any, i: number) => {
          if (thumb.width > biggerWidth) {
            biggerWidth = thumb.width;
            biggerIndex = i;
          }
        });
        const meta = {
          thumbnail: renderer.authorPhoto.thumbnails[biggerIndex].url,
        };
        superchats[id] = {...superchats[id], ...meta};
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
  const unit = match[1].trim();
  const price = parseFloat(match[2].replace(",", ""));
  return {
    amount: price * rate.getCurrentRate(unit),
    unit: unit,
  };
};
