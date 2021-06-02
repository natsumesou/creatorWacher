import axios from "axios";

const CHANNEL_ENDPOINT = "https://www.youtube.com/channel/";

export const findArchivedStream = async (channelId: string) => {
  const response = await fetchVideoArchive(channelId);
  const initData = getInitialJSON(response.data);
  return parseJSONtoFindLatestStream(initData);
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

const parseJSONtoFindLatestStream = (json: any) => {
  const videos = json.contents.twoColumnBrowseResultsRenderer.tabs[1].tabRenderer.content.sectionListRenderer.contents[0];
  if (videos.itemSectionRenderer.contents[0].gridRenderer === undefined) {
    // 動画が1つもない場合(音楽系チャンネルだとたまに検索でヒットするけど動画ページが空の場合がある)
    // TBD
  }
  const latestArchivedStreamRef = videos.itemSectionRenderer.contents[0].gridRenderer.items.find((item: any) => {
    // live予定やプレミアム公開、動画の場合はスキップし続ける
    return item.gridVideoRenderer.publishedTimeText !== undefined &&
      item.gridVideoRenderer.publishedTimeText.simpleText.includes("配信済み");
  });
  const stream = latestArchivedStreamRef.gridVideoRenderer;
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
  return parseInt(str.replace(",", ""));
};

const stringToTimelength = (str: string) => {
  const arr = str.split(":");
  return (+arr[0]) * 60 * 60 + (+arr[1]) * 60 + (+arr[2]);
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
