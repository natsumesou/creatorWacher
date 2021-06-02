import axios from "axios";

const VIDEO_ENDPOINT = "https://www.youtube.com/watch";

export const findChatMessages = async (videoId: string) => {
  await fetchVideoPage(videoId);
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

// @ts-ignore TS6133: 'req' is declared but its value is never read.
const findAPIKey = (html: string) => {
  const match = html.match(/innertubeApiKey":".*?"/);
  if (match === null) {
    return null;
  }
  return match[0].split(":")[1].replace(/"/g, "");
};

// @ts-ignore TS6133: 'req' is declared but its value is never read.
const findContinuation = (html: string) => {
  const match = html.match(/continuation":".*?"/);
  if (match === null) {
    return null;
  }
  return match[0].split(":")[1].replace(/"/g, "");
};
