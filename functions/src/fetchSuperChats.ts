import * as functions from "firebase-functions";
import * as admin from "firebase-admin";

export const fetchSuperChats = async () => {
  const db = admin.firestore();
  const channel = await db.collection("channels").doc("UCS9uQI-jC3DE0L4IpXyvr6w").get().catch((err) => {
    functions.logger.error(err.message + "\n" + err.stack);
  });

  if (!channel || channel && !channel.exists) {
    return;
  }

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
    functions.logger.info(typeof channel.get("superChats"));
  }
};
