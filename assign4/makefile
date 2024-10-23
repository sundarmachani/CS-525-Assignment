CC = gcc
CFLAGS = -g -Wall

all: test_assign4_1 test_assign4_2 test_expr

test_assign4_1: test_assign4_1.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o expr.o record_mgr.o rm_serializer.o btree_mgr.o
	$(CC) $(CFLAGS) -o test_assign4_1 $^

test_assign4_2: test_assign4_2.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o expr.o record_mgr.o rm_serializer.o btree_mgr.o
	$(CC) $(CFLAGS) -o test_assign4_2 $^

test_expr: test_expr.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o expr.o record_mgr.o rm_serializer.o btree_mgr.o
	$(CC) $(CFLAGS) -o test_expr $^

test_assign4_1.o: test_assign4_1.c test_helper.h dberror.h storage_mgr.h buffer_mgr.h buffer_mgr_stat.h expr.h record_mgr.h btree_mgr.h
	$(CC) $(CFLAGS) -c $<

test_assign4_2.o: test_assign4_2.c test_helper.h dberror.h storage_mgr.h buffer_mgr.h buffer_mgr_stat.h expr.h record_mgr.h btree_mgr.h
	$(CC) $(CFLAGS) -c $<

test_expr.o: test_expr.c storage_mgr.h dberror.h buffer_mgr.h buffer_mgr_stat.h expr.h record_mgr.h btree_mgr.h
	$(CC) $(CFLAGS) -c $<

btree_mgr.o: btree_mgr.c btree_mgr.h
	$(CC) $(CFLAGS) -c $<

storage_mgr.o: storage_mgr.c storage_mgr.h
	$(CC) $(CFLAGS) -c $<

dberror.o: dberror.c dberror.h
	$(CC) $(CFLAGS) -c $<

buffer_mgr.o: buffer_mgr.c buffer_mgr.h
	$(CC) $(CFLAGS) -c $<

buffer_mgr_stat.o: buffer_mgr_stat.c buffer_mgr_stat.h
	$(CC) $(CFLAGS) -c $<

expr.o: expr.c expr.h
	$(CC) $(CFLAGS) -c $<

record_mgr.o: record_mgr.c record_mgr.h
	$(CC) $(CFLAGS) -c $<

rm_serializer.o: rm_serializer.c
	$(CC) $(CFLAGS) -c $<

clean: 
	rm -f test_assign4_1 test_assign4_2 test_expr *.o *.bin