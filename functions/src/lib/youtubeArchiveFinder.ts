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
  return {
    videoId: stream.videoId,
    title: stream.title.runs[0].text,
    publishedDateText: stream.publishedTimeText.simpleText.replace(/\sに配信済み/, ""),
    viewCountText: stream.viewCountText.simpleText.replace(/\s回視聴/, ""),
  };
};
