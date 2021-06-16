import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import {ChangeType, migrateSuperChatsToBigQuery} from "./exportToBigQuery";
import {DocumentSnapshot} from "firebase-functions/lib/providers/firestore";

export const migrateToBigQuery = async () => {
  await migrateSuperChats();
};


const migrateSuperChats = async () => {
  const db = admin.firestore();
  const channels = await db.collection("channels").where("category", "in", ["hololive"]).get().catch((err) => {
    functions.logger.error(err.message + "\n" + err.stack);
  });

  if (!channels || channels && channels.empty) {
    return;
  }

  const tempChannels: FirebaseFirestore.QueryDocumentSnapshot<FirebaseFirestore.DocumentData>[] = [];

  channels.forEach((channel) => {
    tempChannels.push(channel);
  });

  for await (const channel of tempChannels) {
    const streams = await channel.ref.collection("streams").get().catch((err) => {
      functions.logger.error(err.message + "\n" + err.stack);
    });

    if (!streams || streams && streams.empty) {
      continue;
    }

    const tempStreams: DocumentSnapshot[] = [];
    streams.forEach((stream) => {
      tempStreams.push(stream);
    });

    for await (const stream of tempStreams) {
      const superChats = await stream.ref.collection("superChats").get().catch((err) => {
        functions.logger.error(err.message + "\n" + err.stack);
      });

      if (!superChats || superChats && superChats.empty) {
        continue;
      }

      let c = 0;
      const tempSuperChats: DocumentSnapshot[] = [];
      superChats.forEach((sc) => {
        if (c < 3) {
          tempSuperChats.push(sc);
          c += 1;
        }
      });
      functions.logger.info("migrate video /" + channel.id + "/streams/" + stream.id + " sc: " + tempStreams.length);
      await migrateSuperChatsToBigQuery(tempSuperChats, channel.id, stream.id, ChangeType.CREATE);
      break;
    }
    break;
  }
};
