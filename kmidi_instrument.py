import argparse
import json
from signal import signal, SIGINT
from sys import exit

from confluent_kafka import Consumer  #, KafkaError
import mido

def handle_arguments():
    parser = argparse.ArgumentParser(description='Plays/consumes MIDI notes from a Kafka topic')

    parser.add_argument("-b", "--bootstrap-servers",
                        help="Bootstrap servers (defaults to 'localhost:9092')",
                        default="localhost:9092")

    parser.add_argument("-t", "--notes_topic",
                        help="Topic to consume notes from (defaults = 'midi_notes')",
                        default="midi_notes")

    # parser.add_argument("--sf2",
    #                     help="SoundFont file to use",
    #                     required=True)
    # --sf2 Steinway_B-JNv2.0.sf2

    return parser.parse_args()


def print_note(note_value):
    notes = ["C", "C#", "D", "D#", "E", "F", "F#", "G", "G#", "A", "A#", "B"]
    # octaves = ["subsubcontra", "sub-contra", "contra", "great", "small", "one-lined", "two-lined", "three-lined", "four-lined", "five-lined", "six-lined"]
    nb_notes = 12
    # 0 to 127 assigned to C1 to G9
    octave = int((note_value / nb_notes)) + 1
    note_index = note_value % nb_notes
    note = notes[note_index]
    print(f"{note} ({octave})")

def sound_note(kafka_msg, outport):
    json_mido_msg = json.loads(kafka_msg.value())
    mido_msg = mido.Message.from_bytes(bytearray.fromhex(json_mido_msg['hex']))
    print_note(mido_msg.note)
    outport.send(mido_msg)


def receive_notes(bootstrap_servers, notes_topic, outport):
    c = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'kmidi',
        'auto.offset.reset': 'latest'
    })

    c.subscribe([notes_topic])

    while True:
        msg = c.poll(5.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        sound_note(msg, outport)

    c.close()


def main():
    def ctrl_c_handler(signal_received, frame):
        # Handle any cleanup here
        print('Thank you for using Kmidi!')
        outport.close()
        exit(0)

    signal(SIGINT, ctrl_c_handler)
    args = handle_arguments()

    with mido.open_output('fluidsynth') as outport:
        print("Waiting for notes...")
        receive_notes(args.bootstrap_servers, args.notes_topic, outport)


if __name__ == "__main__":
    main()
