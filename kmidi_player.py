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
                        help="Folder with MIDI files to play",
                        required=True)

    parser.add_argument("-b", "--bootstrap-servers",
                        help="Bootstrap servers (defaults to 'localhost:9092')",
                        default="localhost:9092")

    parser.add_argument("-t", "--notes-topic",
                        help="Topic to produce (play) notes to (defaults = 'midi_notes')",
                        default="midi_notes")

    group = parser.add_mutually_exclusive_group()
    group.add_argument("-s", "--speed-ratio",
                        help="Speed ratio (1.1 slows down production by 10%%, 0.9 speeds up by 10%%, defaults = 1.0)",
                        default=1.0, type=float)
    group.add_argument('--gg-1955', action='store_true',
                       help="Speed ratio corresponding to Glenn Gould's 1955 intepretation of the Goldberg Variations")
    group.add_argument('--gg-1981', action='store_true',
                       help="Speed ratio corresponding to Glenn Gould's 1981 intepretation of the Goldberg Variations")

    return parser.parse_args()


def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))


def play_notes(producer, topic, midi_file, speed_ratio):
    print(f"Playing {midi_file}... at {speed_ratio}")
    for midi_msg in MidiFile(midi_file):
        time.sleep(midi_msg.time * speed_ratio)

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

    speed_ratio = args.speed_ratio
    if args.gg_1955:
        speed_ratio = 0.65
    elif args.gg_1981:
        speed_ratio = 1.2

    if args.midi_files.find('*') != -1:
        files_to_play = files_from_wildcard(args.midi_files)
    else:
        files_to_play = [args.midi_files]

    p = Producer({'bootstrap.servers': args.bootstrap_servers})
    for f in files_to_play:
        play_notes(producer=p, topic=args.notes_topic, midi_file=f, speed_ratio=speed_ratio)


if __name__ == "__main__":
    main()




if __name__ == '__main__':
    # Tell Python to run the handler() function when SIGINT is recieved

    print('Running. Press CTRL-C to exit.')
    while True:
        # Do nothing and hog CPU forever until SIGINT received.
        pass