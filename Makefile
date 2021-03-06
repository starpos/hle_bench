.PHONY: all clean rebuild test

CXX=g++-4.8.1

ifeq ($(DEBUG),1)
  CFLAGS_OPT = -DDEBUG -g
else
  #CFLAGS_OPT = -DNDEBUG -O2 -pg
  CFLAGS_OPT = -DNDEBUG -O2
endif
CXXFLAGS = $(CFLAGS_OPT) -pthread -std=c++11 -Wall -Wextra
CXXFLAGS += -I./include

#BINARIES = bench test_btree
#BINARIES = bench
BINARIES = test_btree bench bench_map
DEPENDS = $(patsubst %,%.depend,$(BINARIES))

all: $(BINARIES)

%: %.o
	$(CXX) $(CXXFLAGS) -o $@ $<

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $<

clean:
	rm -f $(BINARIES) *.o

rebuild:
	$(MAKE) clean
	$(MAKE) all

%.depend: %.cpp
	$(CXX) -MM $< $(CXXFLAGS) > $@

ifneq "$(MAKECMDGOALS)" "clean"
-include $(DEPENDS)
endif
