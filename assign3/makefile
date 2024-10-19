CC = gcc
CFLAGS = -g -Wall
LDFLAGS = -lm

SRCS = storage_mgr.c dberror.c buffer_mgr.c buffer_mgr_stat.c expr.c record_mgr.c rm_serializer.c
OBJS = $(SRCS:.c=.o)

all: test_assign3_1 test_expr io

test_assign3_1: test_assign3_1.o $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

test_expr: test_expr.o $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

io: interactiveInterface.o $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $<

test_assign3_1.o: test_assign3_1.c test_helper.h storage_mgr.h dberror.h buffer_mgr.h record_mgr.h
	$(CC) $(CFLAGS) -g -c $<

test_expr.o: test_expr.c test_helper.h dberror.h expr.h record_mgr.h tables.h
	$(CC) $(CFLAGS) -g -c $<

interactiveInterface.o: InteractiveInterface.c record_mgr.h
	$(CC) $(CFLAGS) -g -c $<

clean:
	rm -f test_assign3_1 test_expr io *.o *.bin

# New targets
test3: test_assign3_1
	./test_assign3_1

ii: io
	./io

.PHONY: all clean test3 ii