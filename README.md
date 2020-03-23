# kmidi

Kmidi is a set of python scripts that let you hear how your Kafka cluster performance is doing. 

There is a producer script that sends MIDI notes to a topic and there is a consumer script that reads those notes and plays them to your speakers.

When your Kafka cluster is overloaded, the music slows down or the melody gets weird. 

![diagram](kmidi.png)

## Installation

On the instrument side, in addition to python3 and the py libraries used, you'll need a way to turn the MIDI notes into actual sound... For this I'm using <a href='http://www.fluidsynth.org/'>FluidSynth</a>, which uses SoundFont files. You may use any MIDI synth that supports a server mode and opens a MIDI port.

FluidSynth works with SoundFont files that map the MIDI notes to actual sounds. 

I have included a small Stenway piano file (Steinway_B-JNv2.0.sf2). It comes from https://sites.google.com/site/soundfonts4u/ and if you wish to use another one, feel free to substitute it in the command-line examples below. Just remember that you need a SoundFont file that matches the instruments included in the MIDI files.

I have also included MIDI files containing my favorite piano pieces.

To install `kmidi`, download or fork this repo, <a href='https://virtualenv.pypa.io/en/latest/'>create a virtualenv</a> (I recommend using <a href='https://pypi.org/project/virtualenvwrapper/'>virtualenvwrapper</a> over `virtualenv`), and:

```
# once your virtualenv is active:
pip install -r requirements.txt
# if you're on a Mac and use Homebrew, do: 
brew install fluidsynth
```

and test your fluidsynth setup with:

```
fluidsynth Steinway_B-JNv2.0.sf2 midi/bach_988_aria.mid
``` 

You should hear the lovely Aria of the Goldberg Variations by JS Bach.


## Run kmidi

To run kmidi, open 3 terminal windows. (Of course, you also need direct access to a Kafka cluster.)

If topics are not created automatically (they shouldn't be), create one for the MIDI notes with a short retention period. For example:
```
kafka-topics --bootstrap-server localhost:9092 --create --topic midi_notes --config retention.ms=5000 --partitions 12 --replication-factor 3
```

1- Run the synth in server mode (the portname has to be the same as the one referenced in the `mido.open_output()` call in `main()` of `kmidi_instrument.py`):
```
fluidsynth ./Steinway_B-JNv2.0.sf2 --portname=fluidsynth -s
```
 
2- Run the consumer (default values are `--bootstrap-servers localhost:9092  --notes-topic midi_notes`):
```
python kmidi_instrument.py 
 
```
3- run the producer, adjusting the `record size` to match your cluster/application average message size (see Notes below):
```
python kmidi_player.py -m 'midi/*.mid' --record-size 10000
```

You should now hear some nice music. 

Load your Kafka cluster test using `kafka-producer-perf-test` and `kafka-consumer-perf-test`.

The producer sends the notes to the topic respecting the melody. It pauses the proper amount of time before sending the next note(s). The consumer plays the notes as they arrive. So when the music slows down, you know that your cluster throughput is decreasing... If the melody gets wrong, with silences too long  and irregular between notes, it shows a certain inbalance between consumers. 

Et voilà.

See it in action: https://youtu.be/30efBYw5uyU

Notes: 
- You should adjust the number of partitions and replication factor to match your typical cluster setup.
- The script uses the name of the MIDI file as the key so notes should be read in sequence. If you want to hear what happens to the ordering when a consumer is reading from multiple partitions, edit the `producer.produce()` call in `play_notes()` of `kmidi_player.py`, but it may not be pretty to the ear!
- You can adjust the produce speed using the `-s` option. Increase the speed so that the melody is right for your typical load. Then, when the cluster load increases, you'll notice the unusual melody.
- Why use `--record-size`? This is optional but MIDI messages are very small and are written/read very quickly. When these messages are delayed, it's typically by milliseconds and the change in melody may not be noticeable. When this option is used, the player adds a random payload to make the messages bigger.   

## Player options

```
usage: kmidi_player.py [-h] -m MIDI_FILES [-b BOOTSTRAP_SERVERS]
                       [-t NOTES_TOPIC]
                       [-s SPEED_RATIO | --gg-1955 | --gg-1981]
                       [--record-size RECORD_SIZE]

Sends/produces MIDI file notes into a Kafka topic

optional arguments:
  -h, --help            show this help message and exit
  -m MIDI_FILES, --midi-files MIDI_FILES
                        Folder with MIDI files to play
  -b BOOTSTRAP_SERVERS, --bootstrap-servers BOOTSTRAP_SERVERS
                        Bootstrap servers (defaults to 'localhost:9092')
  -t NOTES_TOPIC, --notes-topic NOTES_TOPIC
                        Topic to produce (play) notes to (defaults =
                        'midi_notes')
  -s SPEED_RATIO, --speed-ratio SPEED_RATIO
                        Speed ratio (1.1 slows down production by 10%, 0.9
                        speeds up by 10%, defaults = 1.0)
  --gg-1955             Speed ratio corresponding to Glenn Gould's 1955
                        intepretation of the Goldberg Variations
  --gg-1981             Speed ratio corresponding to Glenn Gould's 1981
                        intepretation of the Goldberg Variations
  --record-size RECORD_SIZE
                        Additional payload to add to each MIDI note message to
                        match the cluster/application average message size
```

## Instrument options

```
usage: kmidi_instrument.py [-h] [-b BOOTSTRAP_SERVERS] [-t NOTES_TOPIC]

Plays/consumes MIDI notes from a Kafka topic

optional arguments:
  -h, --help            show this help message and exit
  -b BOOTSTRAP_SERVERS, --bootstrap-servers BOOTSTRAP_SERVERS
                        Bootstrap servers (defaults to 'localhost:9092')
  -t NOTES_TOPIC, --notes_topic NOTES_TOPIC
                        Topic to consume notes from (defaults = 'midi_notes')

```
