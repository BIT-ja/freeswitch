include $(top_srcdir)/build/modmake.rulesam
MODNAME=mod_rocketmq

if HAVE_ROCKETMQ

mod_LTLIBRARIES = mod_rocketmq.la
mod_rocketmq_la_SOURCES  = mod_rocketmq.cpp
mod_rocketmq_la_CXXFLAGS = $(AM_CXXFLAGS) $(ROCKETMQ_CFLAGS)
mod_rocketmq_la_LIBADD   = $(switch_builddir)/libfreeswitch.la
mod_rocketmq_la_LDFLAGS  = -avoid-version -module -no-undefined -shared $(ROCKETMQ_LIBS) $(SWITCH_AM_LDFLAGS)

else
install: error
all: error
error:
	$(error You must install RocketMQ C++ client library to build this module)
endif
