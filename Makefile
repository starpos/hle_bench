.PHONY: all clean rebuild test

CXX=g++-4.8.1
CXXFLAGS = -O2 -DNDEBUG -pthread -std=c++11 -Wall -Wextra
CXXFLAGS += -I./include

#BINARIES = bench test_btree
#BINARIES = bench
BINARIES = test_btree

all: $(BINARIES)

%: %.o
	$(CXX) $(CXXFLAGS) -o $@ $<

.cpp.o:
	$(CXX) $(CXXFLAGS) -c $<

clean:
	rm -f $(BINARIES) *.o

rebuild:
	$(MAKE) clean
	$(MAKE) all
