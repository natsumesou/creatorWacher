import * as crypto from "crypto";
import axios from "axios";

export const upload = async (text: string, filename: string, directory: string, maxAge = 3600) => {
  const credentials = {
    private_key: "",
    client_email: "",
  };
  const method = "PUT";
  const targetURL = "https://storage.googleapis.com/vtuber.ytubelab.com/" + (directory ? directory : "") + filename;
  const url = getSignedURL(credentials, targetURL, 120, method, "application/octet-stream");
  const options = {
    headers: {
      "content-type": "application/octet-stream",
      "Cache-Control": "public, max-age=" + maxAge,
    },
  };
  try {
    await axios.put(url, text, options);
  } catch (err) {
    console.error(err.message);
  }
};

const getSignedURL = (credential: any, baseURL: string, TTLInSeconds: number, httpVerb: string, contentType: string) => {
  const unixEpochDateInMilliseconds = Date.now();
  const unixEpochDateInSeconds = Math.floor(unixEpochDateInMilliseconds/1000);
  const expiration = unixEpochDateInSeconds + TTLInSeconds;
  const contentMD5 = "";
  const canonicalizedExtensionHeaders = "";
  const canonicalizedResource = baseURL.split("https://storage.googleapis.com")[1];
  const stringToSign = httpVerb + "\n" +
    contentMD5 + "\n" +
    contentType + "\n" +
    expiration + "\n" +
    canonicalizedExtensionHeaders +
    canonicalizedResource;

  const privateKey = credential.private_key;
  const googleAccessStorageId = credential.client_email;
  const signatureBase64Encoded = crypto.createSign("RSA-SHA256").update(stringToSign).sign(privateKey, "base64");
  const signatureURIEncoded = encodeURI(signatureBase64Encoded);

  const params = {
    GoogleAccessId: googleAccessStorageId,
    Expires: expiration,
    Signature: signatureURIEncoded,
  };

  return buildUrl_(baseURL, params);
};

const buildUrl_ = (url:string, params: any) => {
  const paramString = Object.keys(params).map((key) => {
    return encodeURIComponent(key) + "=" + encodeURIComponent(params[key]);
  }).join("&");
  return url + (url.indexOf("?") >= 0 ? "&" : "?") + paramString;
};
