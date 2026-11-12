import { Signal } from "aeolia";
import { createQuery } from "aeolia/contract";

if (process.argv[2] === "state-update") {
  new Signal.State(0).update(null);
} else {
  createQuery({});
}
