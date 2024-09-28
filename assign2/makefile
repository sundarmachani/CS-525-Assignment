# Compiler and flags
CC      = gcc
CFLAGS  = -g -Wall
LDFLAGS = -lm

# Executable names
EXE1    = test1
EXE2    = test2

# Object files
OBJS    = storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o
TEST1_OBJ = test_assign2_1.o
TEST2_OBJ = test_assign2_2.o

# Default target
default: $(EXE1)

# Compile test1
$(EXE1): $(TEST1_OBJ) $(OBJS)
	$(CC) $(CFLAGS) -o $(EXE1) $(TEST1_OBJ) $(OBJS) $(LDFLAGS)

# Compile test2
$(EXE2): $(TEST2_OBJ) $(OBJS)
	$(CC) $(CFLAGS) -o $(EXE2) $(TEST2_OBJ) $(OBJS) $(LDFLAGS)

# Test-specific object file compilation
test_assign2_1.o: test_assign2_1.c dberror.h storage_mgr.h test_helper.h buffer_mgr.h buffer_mgr_stat.h
	$(CC) $(CFLAGS) -c test_assign2_1.c

test_assign2_2.o: test_assign2_2.c dberror.h storage_mgr.h test_helper.h buffer_mgr.h buffer_mgr_stat.h
	$(CC) $(CFLAGS) -c test_assign2_2.c

# Common object file compilation
buffer_mgr_stat.o: buffer_mgr_stat.c buffer_mgr_stat.h buffer_mgr.h
	$(CC) $(CFLAGS) -c buffer_mgr_stat.c

buffer_mgr.o: buffer_mgr.c buffer_mgr.h dt.h storage_mgr.h
	$(CC) $(CFLAGS) -c buffer_mgr.c

storage_mgr.o: storage_mgr.c storage_mgr.h 
	$(CC) $(CFLAGS) -c storage_mgr.c

dberror.o: dberror.c dberror.h 
	$(CC) $(CFLAGS) -c dberror.c

# Clean up generated files
clean: 
	$(RM) $(EXE1) $(EXE2) *.o *~

# Run test1
run_test1: $(EXE1)
	./$(EXE1)

# Run test2
run_test2: $(EXE2)
	./$(EXE2)