import argparse
import json
from signal import signal, SIGINT
from sys import exit
import time


from mido import MidiFile
from confluent_kafka import Producer

from file_wildcards import files_from_wildcard


def handle_arguments():
    parser = argparse.ArgumentParser(description='Sends/produces MIDI file notes into a Kafka topic')

    parser.add_argument("-m", "--midi-files",
                        help="Folder with MIDI files to play")

    parser.add_argument("-b", "--bootstrap-servers",
                        help="Bootstrap servers (defaults to 'localhost:9092')",
                        default="localhost:9092")

    parser.add_argument("-t", "--notes-topic",
                        help="Topic to produce (play) notes to (defaults = 'midi_notes')",
                        default="midi_notes")

    return parser.parse_args()


def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))


def play_notes(producer, topic, midi_file):
    print(f"Playing {midi_file}...")
    for midi_msg in MidiFile(midi_file):
        time.sleep(midi_msg.time)  # adjust with tempo multiplier

        if not midi_msg.is_meta and midi_msg.type in ('note_on', 'note_off'):
            kafka_msg = json.dumps({
                'time': midi_msg.time,
                'type': midi_msg.type,
                'channel': midi_msg.channel,
                'note': midi_msg.note,
                'velocity': midi_msg.velocity,
                'hex': midi_msg.hex(),
            })
            # print(kafka_msg)
            producer.produce(topic, value=kafka_msg.encode('utf-8'), key=midi_file, callback=delivery_report)
            producer.poll(0)


def main():
    def ctrl_c_handler(signal_received, frame):
        # Handle any cleanup here
        print('Thank you for using Kmidi!')
        p.flush()
        exit(0)

    signal(SIGINT, ctrl_c_handler)
    args = handle_arguments()

    if args.midi_files.find('*') != -1:
        files_to_play = files_from_wildcard(args.midi_files)
    else:
        files_to_play = [args.midi_files]

    p = Producer({'bootstrap.servers': args.bootstrap_servers})
    for f in files_to_play:
        play_notes(producer=p, topic=args.notes_topic, midi_file=f)


if __name__ == "__main__":
    main()




if __name__ == '__main__':
    # Tell Python to run the handler() function when SIGINT is recieved

    print('Running. Press CTRL-C to exit.')
    while True:
        # Do nothing and hog CPU forever until SIGINT received.
        pass