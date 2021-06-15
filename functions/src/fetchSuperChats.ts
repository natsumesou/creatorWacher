import * as functions from "firebase-functions";
import * as admin from "firebase-admin";

export const fetchSuperChats = async () => {
  const db = admin.firestore();
  const channels = await db.collection("channels").where("category", "==", "hololive").get().catch((err) => {
    functions.logger.error(err.message + "\n" + err.stack);
  });

  if (!channels || channels && channels.empty) {
    return;
  }

  const tempChannels: FirebaseFirestore.QueryDocumentSnapshot<FirebaseFirestore.DocumentData>[] = [];

  channels.forEach((channel) => {
    tempChannels.push(channel);
  });

  for (const channel of tempChannels) {
    const streams = await channel.ref.collection("streams").get().catch((err) => {
      functions.logger.error(err.message + "\n" + err.stack);
    });

    if (!streams || streams && streams.empty) {
      return;
    }

    const tempStreams: FirebaseFirestore.QueryDocumentSnapshot<FirebaseFirestore.DocumentData>[] = [];
    streams.forEach((stream) => {
      tempStreams.push(stream);
    });

    for (const stream of tempStreams) {
      functions.logger.info(channel.get("title") + ":" + stream.get("superChats"));
      functions.logger.info(typeof stream.get("superChats"));
      break;
      // if () {
      //   // await publishAnalyzeStream(doc.id, channel.id);
      // }
    }
    break;
  }
};
