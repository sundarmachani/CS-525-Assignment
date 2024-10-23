#include "dberror.h"
#include "expr.h"
#include "btree_mgr.h"
#include "tables.h"
#include "test_helper.h"

#define ASSERT_EQUALS_RID(_l,_r, message)				\
  do {									\
    ASSERT_TRUE((_l).page == (_r).page && (_l).slot == (_r).slot, message); \
  } while(0)


// test methods
static void testIntKeys(void);
static void testFloatKeys(void);
static void testStringKeys(void);
static void testBoolKeys(void);
static void testMixedOperations(void);

// test name
char *testName;

int main(void) {
    testName = "";
    
    testIntKeys();
    testFloatKeys();
    testStringKeys();
    testBoolKeys();
    testMixedOperations();
    
    return 0;
}

// Test integer keys
void testIntKeys(void) {
    testName = "test integer keys";
    
    BTreeHandle *tree = NULL;
    Value *keys[5];
    RID rids[] = {{1,1}, {2,2}, {3,3}, {4,4}, {5,5}};
    
    // Create integer keys
    for(int i = 0; i < 5; i++) {
        keys[i] = (Value *) malloc(sizeof(Value));
        keys[i]->dt = DT_INT;
        keys[i]->v.intV = i * 10;  // Values: 0, 10, 20, 30, 40
    }
    
    // Initialize and create tree
    TEST_CHECK(initIndexManager(NULL));
    TEST_CHECK(createBtree("testInt", DT_INT, 2));
    TEST_CHECK(openBtree(&tree, "testInt"));
    
    // Insert keys
    for(int i = 0; i < 5; i++)
        TEST_CHECK(insertKey(tree, keys[i], rids[i]));
    
    // Search for keys
    RID rid;
    for(int i = 0; i < 5; i++) {
        TEST_CHECK(findKey(tree, keys[i], &rid));
        ASSERT_EQUALS_RID(rids[i], rid, "wrong RID for integer key");
    }
    
    // Cleanup
    TEST_CHECK(closeBtree(tree));
    TEST_CHECK(deleteBtree("testInt"));
    for(int i = 0; i < 5; i++)
        free(keys[i]);
    
    TEST_DONE();
}

// Test float keys
void testFloatKeys(void) {
    testName = "test float keys";
    
    BTreeHandle *tree = NULL;
    Value *keys[5];
    RID rids[] = {{1,1}, {2,2}, {3,3}, {4,4}, {5,5}};
    
    // Create float keys
    for(int i = 0; i < 5; i++) {
        keys[i] = (Value *) malloc(sizeof(Value));
        keys[i]->dt = DT_FLOAT;
        keys[i]->v.floatV = i * 1.5;  // Values: 0.0, 1.5, 3.0, 4.5, 6.0
    }
    
    TEST_CHECK(initIndexManager(NULL));
    TEST_CHECK(createBtree("testFloat", DT_FLOAT, 2));
    TEST_CHECK(openBtree(&tree, "testFloat"));
    
    // Insert and search
    for(int i = 0; i < 5; i++)
        TEST_CHECK(insertKey(tree, keys[i], rids[i]));
    
    RID rid;
    for(int i = 0; i < 5; i++) {
        TEST_CHECK(findKey(tree, keys[i], &rid));
        ASSERT_EQUALS_RID(rids[i], rid, "wrong RID for float key");
    }
    
    // Cleanup
    TEST_CHECK(closeBtree(tree));
    TEST_CHECK(deleteBtree("testFloat"));
    for(int i = 0; i < 5; i++)
        free(keys[i]);
    
    TEST_DONE();
}

// Test string keys
void testStringKeys(void) {
    testName = "test string keys";
    
    BTreeHandle *tree = NULL;
    Value *keys[5];
    RID rids[] = {{1,1}, {2,2}, {3,3}, {4,4}, {5,5}};
    char *strings[] = {"apple", "banana", "cherry", "date", "elderberry"};
    
    // Create string keys
    for(int i = 0; i < 5; i++) {
        keys[i] = (Value *) malloc(sizeof(Value));
        keys[i]->dt = DT_STRING;
        keys[i]->v.stringV = strdup(strings[i]);
    }
    
    TEST_CHECK(initIndexManager(NULL));
    TEST_CHECK(createBtree("testString", DT_STRING, 2));
    TEST_CHECK(openBtree(&tree, "testString"));
    
    // Insert and search
    for(int i = 0; i < 5; i++)
        TEST_CHECK(insertKey(tree, keys[i], rids[i]));
    
    RID rid;
    for(int i = 0; i < 5; i++) {
        TEST_CHECK(findKey(tree, keys[i], &rid));
        ASSERT_EQUALS_RID(rids[i], rid, "wrong RID for string key");
    }
    
    // Cleanup
    TEST_CHECK(closeBtree(tree));
    TEST_CHECK(deleteBtree("testString"));
    for(int i = 0; i < 5; i++) {
        free(keys[i]->v.stringV);
        free(keys[i]);
    }
    
    TEST_DONE();
}

// Test boolean keys
void testBoolKeys(void) {
    testName = "test boolean keys";
    
    BTreeHandle *tree = NULL;
    Value *keys[2];
    RID rids[] = {{1,1}, {2,2}};
    
    // Create boolean keys
    for(int i = 0; i < 2; i++) {
        keys[i] = (Value *) malloc(sizeof(Value));
        keys[i]->dt = DT_BOOL;
        keys[i]->v.boolV = (i == 0) ? TRUE : FALSE;
    }
    
    TEST_CHECK(initIndexManager(NULL));
    TEST_CHECK(createBtree("testBool", DT_BOOL, 2));
    TEST_CHECK(openBtree(&tree, "testBool"));
    
    // Insert and search
    for(int i = 0; i < 2; i++)
        TEST_CHECK(insertKey(tree, keys[i], rids[i]));
    
    RID rid;
    for(int i = 0; i < 2; i++) {
        TEST_CHECK(findKey(tree, keys[i], &rid));
        ASSERT_EQUALS_RID(rids[i], rid, "wrong RID for boolean key");
    }
    
    // Cleanup
    TEST_CHECK(closeBtree(tree));
    TEST_CHECK(deleteBtree("testBool"));
    for(int i = 0; i < 2; i++)
        free(keys[i]);
    
    TEST_DONE();
}

// Test mixed operations
void testMixedOperations(void) {
    testName = "test mixed operations with different types";
    
    BTreeHandle *tree = NULL;
    Value *keys[4];
    RID rids[] = {{1,1}, {2,2}, {3,3}, {4,4}};
    
    // Create keys of different types
    keys[0] = (Value *) malloc(sizeof(Value));
    keys[0]->dt = DT_INT;
    keys[0]->v.intV = 42;
    
    keys[1] = (Value *) malloc(sizeof(Value));
    keys[1]->dt = DT_FLOAT;
    keys[1]->v.floatV = 3.14;
    
    keys[2] = (Value *) malloc(sizeof(Value));
    keys[2]->dt = DT_STRING;
    keys[2]->v.stringV = strdup("test");
    
    keys[3] = (Value *) malloc(sizeof(Value));
    keys[3]->dt = DT_BOOL;
    keys[3]->v.boolV = TRUE;
    
    // Test each type in separate trees
    TEST_CHECK(initIndexManager(NULL));
    
    // Test int tree
    TEST_CHECK(createBtree("testInt", DT_INT, 2));
    TEST_CHECK(openBtree(&tree, "testInt"));
    TEST_CHECK(insertKey(tree, keys[0], rids[0]));
    RID rid;
    TEST_CHECK(findKey(tree, keys[0], &rid));
    ASSERT_EQUALS_RID(rids[0], rid, "wrong RID for int key");
    TEST_CHECK(closeBtree(tree));
    TEST_CHECK(deleteBtree("testInt"));
    
    // Test float tree
    TEST_CHECK(createBtree("testFloat", DT_FLOAT, 2));
    TEST_CHECK(openBtree(&tree, "testFloat"));
    TEST_CHECK(insertKey(tree, keys[1], rids[1]));
    TEST_CHECK(findKey(tree, keys[1], &rid));
    ASSERT_EQUALS_RID(rids[1], rid, "wrong RID for float key");
    TEST_CHECK(closeBtree(tree));
    TEST_CHECK(deleteBtree("testFloat"));
    
    // Cleanup
    free(keys[2]->v.stringV);
    for(int i = 0; i < 4; i++)
        free(keys[i]);
    
    TEST_CHECK(shutdownIndexManager());
    TEST_DONE();
}