import os
import re
import sys
import json
import time
import subprocess
import uuid
import argparse
import tempfile
import traceback
from io import StringIO
from concurrent.futures import ThreadPoolExecutor

#TODO: Encapsulate things more (like AvgCounter is)
#parallel echo {} {/} {/.} ::: dtest_com/*.mp4

#Allowed video extensions
#TODO: Allow selecting via argument
videxts = (
	".webm",
	".mkv",
	".flv",
	".vob",
	".ogv",".ogg",
	".drc",
	".mng",
	".avi",
	".mts",".m2ts",".ts",
	".mov",".qt",
	".wmv",
	".yuv",
	".rm",
	".rmvb",
	".asf",
	".amv",
	".mp4",".m4p",".m4v",
	".mpg",".mp2",".mpeg",".mpe",".mpv",
	".m2v",
	".svi",
	".3gp",
	".3g2",
	".mxf",
	".roq",
	".nsv",
	".flv",".f4v",".f4p",".f4a",".f4b",
	".gifv", #Im sorry little one, use apng instead
)

#Thank: https://stackoverflow.com/a/59678681
class Range(object):
	def __init__(self, start, end):
		self.start = start
		self.end = end
	def __eq__(self, other):
		return self.start <= other <= self.end
	def __contains__(self, item):
		return self.__eq__(item)
	def __iter__(self):
		yield self
	def __str__(self):
		return '{0}..{1}'.format(self.start, self.end)

#Thank: https://stackoverflow.com/a/11415816
#TODO: Check write/read-ability
class readable_dir(argparse.Action):
	def __call__(self, parser, namespace, values, option_string=None):
		prospective_dir=values
		if not os.path.isdir(prospective_dir):
			raise argparse.ArgumentTypeError("readable_dir:{0} is not a valid path".format(prospective_dir))
		if os.access(prospective_dir, os.R_OK):
			setattr(namespace,self.dest,prospective_dir)
		else:
			raise argparse.ArgumentTypeError("readable_dir:{0} is not a readable dir".format(prospective_dir))

parser = argparse.ArgumentParser(prog="hevcify",description="Recursively convert a folder to HEVC/H.265",formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("path", type=str, action=readable_dir, help="Input folder path")
parser.add_argument("--tmp", type=str, default=tempfile.gettempdir(), action=readable_dir, help="Where to store temporary files")
parser.add_argument("--size_reduction", type=float, default=0.15, choices=Range(0.0,1.0), help="The minimum percentage to save (0.2 for 20%% smaller)")
parser.add_argument("--delete", action="store_true", help="Delete original files")
parser.add_argument("--iterations", type=int, default=60, choices=Range(1,5*60), help="Number of seconds to keep in the bitrate counter (1 per second)")
parser.add_argument("--type", type=str, default="mp4", choices=[k[1:] for k in videxts], help="Filetype (mkv, mp4, etc.)")
parser.add_argument("--same_type", action="store_true", help="Keep file extension given by --same_type_filter")
parser.add_argument("--same_type_filter", type=str, default="mkv,mp4,m4v,mov", help="File extensions to keep given --same_type")
parser.add_argument("--repair", action="store_true", help="When finding invalid files, attempt to remux with ffmpeg to fix")
parser.add_argument("--ignore_hevc", action="store_true", help="Ignore files already encoded in HEVC/H.265")
parser.add_argument("--workers", type=int, default=1, choices=Range(1,32), help="Number of ffmpeg instances to run")
parser.add_argument("--stop_at", type=float, default=0.45, choices=Range(0.0,1.0), help="Percentage number when to finish encoding")
#TODO WONTFIX: If im ever gonna publish this, i should add AMD support
#	I dont own an amd card thought, hopefully it has CRF
#	hevc_amf apparently
#	Oh my god it only has qp
#	fuck amd, im not supporting it
parser.add_argument("--nvenc", action="store_true", help="Uses NVENC instead of libx265")
#TODO: libx265/nvenc custom arguments?
#TODO: Force reencode?
#TODO: Some ignore predicate, like ignore HEVC, but more general

args = parser.parse_args()

print(args)

#TODO: Handle ffmpeg check gracefully
subprocess.check_call(["ffmpeg","-version"], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
subprocess.check_call(["ffprobe","-version"], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
#TODO: If nvenc check for NVIDIA Video Codec SDK 11

def get(file):
	return json.loads(subprocess.check_output([
		"ffprobe",
		"-v","quiet",
		"-print_format","json",
		"-show_format", "-show_streams",
		"-sexagesimal",
		file
	]))

def process_read(proc):
	string = b""

	while True:
		byte = proc.stdout.read(1)
		if not byte:
			break
		string += byte
		if (byte == b"\r" or byte == b"\n"):
			yield string
			string=b""

#python's datetime is a fucking terrible library holy shit
def toseconds(time):
	c = re.match(r"(\d+):(\d+):(\d+)(\.\d+)?",time)
	c = float(c.group(3)) + float(c.group(2)) * 60 + float(c.group(1)) * 60 * 60
	return int(c)

def getprogressbar(cur,max,len=60):
	len=len-2
	progress = cur/max
	prss = int(len*progress)
	return "[{}{}]".format(
		"#"*prss,
		" "*(len - prss)
	)

#Thank: https://stackoverflow.com/questions/12523586/python-format-size-application-converting-b-to-kb-mb-gb-tb
def format_bytes(size):
	# 2**10 = 1024
	power = 2**10
	n = 0
	power_labels = {0 : '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
	while size > power:
		size /= power
		n += 1
	return size, power_labels[n]+'B'

class AvgCounter:
	def __init__(self, tlen, tmin):
		self.tmin = tmin
		self.tlen = tlen
		self.tbl = [0 for i in range(0,self.tlen)]
		self.idx = 0

	def add(self, val):
		self.tbl[self.idx] = val
		self.idx += 1
		if (self.idx >= self.tlen):
			self.idx = 0

		avg = sum(self.tbl)/len(self.tbl)
		if (avg > self.tmin):
			return True

		return False

class WorkLog:
	def __init__(self):
		self.fp = open("hevcify.log","a+")
		self.tbl = []

		self.fp.seek(0)
		for i in self.fp.readlines():
			self.tbl.append(i.strip()) #Remove newline

	def exists(self,i):
		return i in self.tbl
	def add(self,i):
		self.tbl.append(i)
		self.fp.write("{}\n".format(i))
	def close(self):
		self.fp.close()

#TODO: does tempfile allow us to keep the file?
def gettempfile(ext):
	return os.path.join(args.tmp,"{}.{}".format(uuid.uuid4(),ext))

def dowork(in_file, stdout):
	filename = in_file[in_file.rfind("/")+1:]
	ext = filename[filename.rfind(".")+1:]

	if (args.same_type):
		out_file = gettempfile(ext if ext in args.same_type_filter.split(",") else "mp4")
	else:
		out_file = gettempfile(args.type)

	vid_data = get(in_file)
	#TODO: Fail on invalid

	if args.ignore_hevc:
		for i in vid_data["streams"]:
			if i["codec_type"] == "video" and (i["codec_name"].lower() == "hevc" or i["codec_name"].lower() == "h265"):
				return None, None

	stdout.write(">========================================>\n")

	if not "bit_rate" in vid_data["format"] or not "duration" in vid_data["format"]:
		if args.repair:
			tmp_file = gettempfile(ext)
			stdout.write("File is corrupt, attempting ffmpeg remux...\n")
			#mkvpropedit --add-track-statistics-tags
			subprocess.check_call([
				"ffmpeg", 
				"-hwaccel","auto",
				"-i", in_file,
				"-c","copy",
				tmp_file
			])
			os.remove(in_file)
			os.rename(tmp_file,in_file)
			vid_data = get(in_file)
			#TODO: Check again, but in a nice way
		else:
			stdout.write("File is corrupt, skipping...\n")
			return None, None

	#TODO: Remove this
	"""for i in vid_data["streams"]:
		if i["codec_type"] == "video":
			if "tags" in i and "ENCODER" in i["tags"] and "libx265" in i["tags"]["ENCODER"]:
				#TODO: Doesn't work on mp4 files?
				return None, None"""

	#TODO: Print ffmpeg output to a debug log
	#stdout.write(json.dumps(vid_data,indent="\t"))

	stdout.write("Name: {}\n".format(filename))
	#TODO: Input video codec?
	stdout.write("Path: {}\n".format(in_file))
	og_size = int(vid_data["format"]["size"])
	size, label = format_bytes(og_size)
	stdout.write("Size: {:.2f}{}\n".format(size, label))
	og_bitrate = int(vid_data["format"]["bit_rate"])
	pix_fmt = None
	for i in vid_data["streams"]:
		if i["codec_type"] == "video":
			pix_fmt = i["pix_fmt"]
			stdout.write("Codec: {}\n".format(i["codec_name"]))
			stdout.write("Resolution: {}x{}\n".format(i["width"], i["height"]))
			break
	stdout.write("Bitrate: {:.2f}kbits/s\n".format(og_bitrate/1000.0))
	stdout.write("Length: {}\n".format(str(vid_data["format"]["duration"])))
	stdout.write("Temporary file: {}\n".format(out_file))
	og_lenght = toseconds(vid_data["format"]["duration"])

	#TODO: HDR Support
	hevc_nvenc_args = [
		"ffmpeg",
		"-hwaccel","auto",
		"-i",in_file,
		"-map","0", #Keep all data
		"-c:a","copy",
		"-c:v","hevc_nvenc",
		"-preset","p7", 
		#Slowest possible, 
		# also only available if compiled against NVIDIA Video Codec SDK 11 (or 10?)
		"-rc","vbr",
		"-rc-lookahead","32",
		"-tune","hq",
		"-b_ref_mode","each",
		"-bf","5",
		"-cq","25",
		"-pix_fmt",pix_fmt, #Yeah this might become cpu limited, but thats on the FFMPEG team to fix
		out_file
	]
	#NVIDIA H264 max: 4096x4096
	#NVIDIA H265 max: 8192x8192

	#Tested on TU116 (GTX 1660 SUPER) on 30s clips
	#ffmpeg version is 4.4 < git version
	#2160p:
	#NVIDIA VMAF cq  26 preset p7     - 97.458816
	#NVIDIA VMAF cq  25 preset p7     - 97.652741
	#X265   VMAF crf 18 preset medium - 97.442563, about 2% bigger than nvidia
	#X265   VMAF crf 18 preset slow   - 97.695730, about 30% bigger than nvidia
	#1080p:
	#NVIDIA VMAF cq  25 - 98.634269
	#X265   VMAF crf 18 - 98.635478, about same size, maybe 1% worse

	#ffmpeg -hwaccel auto -i input.mp4 -hwaccel auto -i nvenc.mp4 -lavfi libvmaf -f null -
	#ffmpeg -hwaccel auto -i input.mp4 -hwaccel auto -i x265.mp4 -lavfi libvmaf -f null -

	libx265_args = [
		"ffmpeg",
		"-hwaccel","auto",
		"-i",in_file,
		"-map","0", #Keep all data
		"-c:a","copy",
		"-c:v","libx265",
		"-crf","18",
		"-preset","slow", #TODO: Customize
		"-x265-params","wpp=1:pmode=1", #:pme=1:frame-dup=1
		"-pix_fmt",pix_fmt,
		out_file
	]

	arg = []
	if (args.nvenc):
		arg = hevc_nvenc_args
	else:
		arg = libx265_args

	proc = subprocess.Popen(arg, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
	bits = AvgCounter(args.iterations, og_bitrate*(1-args.size_reduction))

	for string in process_read(proc):
		#TODO: Any script errors fail silently, fail loudly
		#TODO: Delete temporary file on failure
		#TODO: Print ffmpeg output to a debug log

		#Quit and delete new if new average bitrate is N% above old one
		r = re.match(r"frame=(.*)fps=(.*)q=(.*)size=(.*)time=(.*)bitrate=(.*)kbits\/s.*speed=(.*)x.*", string.decode("utf8").strip())
		if r:
			#sys.stdout.write(string + "\r")
			#TODO: Catch CTRL+C and delete temporary file
			new_kbits = float(r.group(6))
			new_bitrate = int(new_kbits*1000)

			current_length = toseconds(r.group(5).strip())
			progress = None
			if (current_length > 0):
				progress = current_length/og_lenght
				#BUG: If current line get smaller, it leaves artifacts
				out = " prog={:01.02f}% time={} bitrate={}kbits/s speed={}x\r".format(
					progress*100,
					r.group(5),
					r.group(6),
					r.group(7)
				)
				width = os.get_terminal_size().columns + 1 - len(out)
				if (width < 2):
					width = 2

				out = "{}{}".format(getprogressbar(current_length,og_lenght, len=width), out)

				stdout.write(out)

			stdout.flush()

			beyond_stop = progress != None and args.stop_at < progress
			if (bits.add(new_bitrate) and not beyond_stop):
				proc.terminate()
				stdout.write("\n")
				return False, out_file
	stdout.write("\n")

	#Wait for termination...
	outs, errs = proc.communicate()

	#Check for errors
	if proc.returncode != 0:
		return None, out_file

	#Quit and delete new if newly encoded file size is not N% smaller
	new_size = os.path.getsize(out_file)
	old_size = os.path.getsize(in_file)
	savings = new_size/old_size
	if (savings >= 1-args.size_reduction):
		return False, out_file

	return True, out_file

#TODO: Turn these 2 functions into their own class
def doresult(in_file, stdout):
	good, tmpfile = dowork(in_file, stdout)

	if (tmpfile != None): 
		stdout.write("Is smaller: {}, Temporary File: {}\n".format(good,tmpfile))
		#Delete old and rename
		if (good == None):
			stdout.write("Failure...\n")
			os.remove(tmpfile) #TODO: No such file?
			return True

		if (good):
			size, label = format_bytes(os.path.getsize(tmpfile))
			stdout.write("New Size: {:.2f}{}\n".format(size, label))
			size, label = format_bytes(os.path.getsize(in_file) - os.path.getsize(tmpfile))
			shavings = (1-(os.path.getsize(tmpfile)/os.path.getsize(in_file))) * 100
			#Hehe
			#Shavings
			stdout.write("Shavings: {:.2f}{} {:.2f}%\n".format(size, label, shavings))

			stdout.write("Replacing...\n")
			if args.delete:
				os.remove(in_file)
			else:
				os.rename(in_file, in_file + ".old")
			where = in_file[:in_file.rfind(".")] + tmpfile[tmpfile.rfind("."):]

			stdout.write("New file: {}\n".format(where))
			os.rename(tmpfile, where)
		else:
			stdout.write("Keeping...\n")
			os.remove(tmpfile)
	return False

def read_io(log, isdone):
	i = 0
	while True: #Where did the do while loop go????
		text = log.getvalue()

		if (len(text) > i):
			yield text[i:]
			i = len(text)
		else:
			#Speedcap
			time.sleep(0.1)

		if isdone():
			yield text[i:] + "\n" #Return any remaining
			break

def run():
	wrk = WorkLog()
	#TODO: Save log to file
	# maybe wrap in a StringIO?
	# remember to truncate all \r to only the last instance of a sequence of \r

	with ThreadPoolExecutor(max_workers=args.workers) as executor:
		futures = []

		for root, dirs, files in os.walk(args.path, topdown=True):
			for name in files:
				in_file = os.path.join(root, name)
				ext = name[name.rfind("."):]

				if not ext in videxts:
					continue

				if wrk.exists(in_file):
					continue

				#TODO: Find a more efficient scheme
				log = StringIO()
				futures.append([log, in_file, executor.submit(doresult, in_file, log)])

		while (len(futures) > 0):
			current = futures[0]

			log, in_file, future = current
			for i in read_io(log, lambda: future.done()):
				sys.stdout.write(i)

			try:
				future.result()
			except Exception as e:
				print(e)

			wrk.add(in_file) #TODO: Detect ffmpeg failure
			log.close()

			futures.remove(current) #TODO: Too inefficient?

	wrk.close()

if __name__ == "__main__":
	run()
