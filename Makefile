CFLAGS+=-Wall -Wextra -g -std=c99 `mysql_config --cflags` `pkg-config glib-2.0 gthread-2.0 --cflags` -Wno-implicit-fallthrough
LDLIBS+=`mysql_config --libs_r | sed -e s/zlib/z/` `pkg-config glib-2.0 gthread-2.0 --libs` -lcrypto

table_copy: table_copy.o queue.o

clean:
	rm -f *.o table_copy
