from time import sleep  
from raw_producer import raw_producer

if __name__ == '__main__':
	while True:
		sleep(600)
		# 有先后顺序，无法实现
		raw_producer()
		parse_producer()