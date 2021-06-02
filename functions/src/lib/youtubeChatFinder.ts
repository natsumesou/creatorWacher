import axios from "axios";

const VIDEO_ENDPOINT = "https://www.youtube.com/watch";
const CHAT_ENDPOINT = "https://www.youtube.com/youtubei/v1/live_chat/get_live_chat_replay";

export const findChatMessages = async (videoId: string) => {
  const response = await fetchVideoPage(videoId);

  const apiKey = findKey("INNERTUBE_API_KEY", response.data);
  const continuation = findContinuation("continuation", response.data);
  const visitor = findKey("visitorData", response.data);
  const client = findKey("clientVersion", response.data);

  const chatmeta = await fetchChatData(apiKey, continuation, visitor, client);
  for (const cont of chatmeta.data.continuationContents.liveChatContinuation.continuations) {
    if (cont.liveChatReplayContinuationData === undefined) {
      continue;
    }
    const continuationNext = cont.liveChatReplayContinuationData.continuation;
    const chatmeta2 = await fetchChatData(apiKey, continuationNext, visitor, client);
    console.log(JSON.stringify(chatmeta2.data.continuationContents.liveChatContinuation.continuations));
  }
  // console.log(JSON.stringify(chatmeta.data.continuationContents.liveChatContinuation.actions[1]));
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
  return findVariable(keyName, sourcee, 100); // continuationが複数あり、小さい値の方は偽物なので弾く。
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
