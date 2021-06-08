import axios from "axios";

export const CHANNEL_ENDPOINT = "https://www.youtube.com/channel/";

export const findArchivedStreams = async (channelId: string) => {
  const response = await fetchVideoArchive(channelId);
  const initData = getInitialJSON(response.data);
  const streams = parseJSONtoFindStreams(initData);
  const subscribeCount = parseJSONtoFindSubscribers(initData);
  const result = {
    subscribeCount: subscribeCount,
    streams: streams,
  };
  return result;
};

const fetchVideoArchive = async (channelId: string) => {
  const params = {
    "view": "0",
    "sort": "dd",
    "flow": "grid",
  };
  const headers = {
    "Cookie": "PREF=\"tz=Asia.Tokyo&hl=ja\";",
  };
  return await axios.get(CHANNEL_ENDPOINT + channelId + "/videos", {
    params: params,
    headers: headers,
  });
};

const getInitialJSON = (html: string) => {
  const match = html.match(/var ytInitialData = (.+);<\/script>/);
  if (match === null) {
    return null;
  }
  return JSON.parse(match[1]);
};

const parseJSONtoFindSubscribers = (json: any) => {
  const subscriberStr = json.header.c4TabbedHeaderRenderer.subscriberCountText.simpleText.replace(/チャンネル登録者数\s/, "").replace(/人/, "");
  return kanjiToNum(subscriberStr);
};

const parseJSONtoFindStreams = (json: any) => {
  if (!json.contents.twoColumnBrowseResultsRenderer) {
    throw new Error("why ytInitialData is empty?\n" + JSON.stringify(json));
  }
  const videos = json.contents.twoColumnBrowseResultsRenderer.tabs[1].tabRenderer.content.sectionListRenderer.contents[0];
  if (videos.itemSectionRenderer.contents[0].gridRenderer === undefined) {
    // 動画が1つもない場合(音楽系チャンネルだとたまに検索でヒットするけど動画ページが空の場合がある)
  }
  const streams = videos.itemSectionRenderer.contents[0].gridRenderer.items.reduce((result: Array<any>, item: any) => {
    // live予定やプレミアム公開、動画の場合はスキップし続ける
    const publishedDateText = item.gridVideoRenderer?.publishedTimeText?.simpleText || "";
    if (publishedDateText.includes("配信済み") && isRecursiveLimit(publishedDateText)) {
      result.push(item.gridVideoRenderer);
    }
    return result;
  }, []);
  return streams.map((stream:any) => formatStream(stream));
};

const formatStream = (stream: any) => {
  const now = new Date();
  const publishedDate = stringToDatetime(stream.publishedTimeText.simpleText.replace(/\sに配信済み/, ""), now);
  const viewCount = stringToNum(stream.viewCountText.simpleText.replace(/\s回視聴/, ""));
  const overlay = stream.thumbnailOverlays.find((overlay: any) => {
    return overlay.thumbnailOverlayTimeStatusRenderer !== undefined;
  });
  const streamLengthSec = stringToTimelength(overlay.thumbnailOverlayTimeStatusRenderer.text.simpleText);
  return {
    id: stream.videoId,
    title: stream.title.runs[0].text,
    viewCount: viewCount,
    streamLengthSec: streamLengthSec,
    publishedAt: publishedDate,
    createdAt: now,
  };
};

/**
 * 一ヶ月前までのアーカイブを取得する
 * @param {string} text アーカイブの配信日時のテキスト
 * @return {boolean} 取得すべきかを返す
 */
const isRecursiveLimit = (text: string) => {
  return text.includes("秒前") ||
  text.includes("分前") ||
  text.includes("時間前") ||
  text.includes("日前") ||
  text.includes("週間前");
};

const stringToDatetime = (relative: string, now: Date) => {
  const match = relative.match(/(\d+)+\s(.+)/);
  if (match === null) {
    throw new Error("配信終了日時の変換に失敗しました");
  }
  const num = parseInt(match[1]);
  const seconds = stringToSecond(num, match[2]);

  const copiedNow = new Date(now.getTime());
  copiedNow.setSeconds(copiedNow.getSeconds() - seconds);
  return copiedNow;
};

const stringToNum = (str: string) => {
  const re = new RegExp(/,/, "g");
  return parseInt(str.replace(re, ""));
};

const stringToTimelength = (str: string) => {
  const arr = str.split(":");
  if (arr.length === 1) {
    return +arr[0];
  } else if (arr.length === 2) {
    return (+arr[0]) * 60 + (+arr[1]);
  } else if (arr.length === 3) {
    return (+arr[0]) * 60 * 60 + (+arr[1]) * 60 + (+arr[2]);
  } else {
    throw new Error("配信時間の処理中にエラーが発生しました: " + str);
  }
};

const stringToSecond = (num: number, unit: string) => {
  switch (unit) {
    case "秒前":
      return num;
    case "分前":
      return num * 60;
    case "時間前":
      return num * 60 * 60;
    case "日前":
      return num * 60 * 60 * 24;
    case "週間前":
      return num * 60 * 60 * 24 * 7;
    case "か月前":
      return num * 60 * 60 * 24 * 30;
    case "年前":
      return num * 60 * 60 * 24 * 365;
    default:
      throw new Error("配信終了日時の変換で想定外の単位が出現しました:" + unit);
  }
};

const kanjiToNum = (str: string) => {
  const match = str.match(/([\d.]+)(.+)?/);
  if (match === null) {
    throw new Error("チャンネル登録者数の処理中にエラーが発生しました:" + str);
  }
  const num = parseFloat(match[1]);
  return kanjiToNumUnit(num, match[2]);
};

const kanjiToNumUnit = (num: number, unit: string) => {
  switch (unit) {
    case "万":
      return num * 10000;
    case "億":
      return num * 100000000;
    case undefined:
      return num;
    default:
      throw new Error("登録者数の変換で想定外の単位が出現しました:" + unit);
  }
};
