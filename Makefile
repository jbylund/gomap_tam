PG_CONFIG   ?= pg_config
PGXS        := $(shell $(PG_CONFIG) --pgxs)
PKGLIBDIR   := $(shell $(PG_CONFIG) --pkglibdir)

# Go shared library
SHIM_DIR    := go
SHIM_SO     := $(PKGLIBDIR)/treedb_shim.so

# iceoryx2-c paths (built in-tree from the cloned repo)
IOX2_ROOT   := $(CURDIR)/../iceoryx2
IOX2_BUILD  := $(IOX2_ROOT)/target/ff/cc/build/rust/native/release
IOX2_INC    := $(IOX2_BUILD)/iceoryx2-ffi-c-cbindgen/include
IOX2_LIB    := $(IOX2_BUILD)/libiceoryx2_ffi_c.a

# Compiler flags: include iceoryx2 headers + pass shim path.
PG_CPPFLAGS := -I$(CURDIR) -I$(IOX2_INC) -DTDB_SHIM_PATH=\"$(SHIM_SO)\"

# Extra linker flags: link iceoryx2 static library.
# -lm -ldl -lpthread are needed by the Rust runtime inside the static lib.
SHLIB_LINK  := $(IOX2_LIB) -lm -ldl -lpthread

MODULE_big  := treedb_pgext
OBJS        := treedb_tam.o treedb_bgworker.o
EXTENSION   := treedb_pgext
DATA        := treedb_pgext--1.0.sql

include $(PGXS)

# Build the Go shared library before the C objects.
$(SHIM_SO): $(SHIM_DIR)/treedb_shim.go $(SHIM_DIR)/go.mod
	@echo "==> Building Go shim..."
	cd $(SHIM_DIR) && \
	  CGO_ENABLED=1 go build \
	    -buildmode=c-shared \
	    -o $(SHIM_SO) \
	    .
	@test -f $(SHIM_DIR)/treedb_shim.h && mv $(SHIM_DIR)/treedb_shim.h /dev/null || true
	@echo "==> Go shim installed to $(SHIM_SO)"

# Make sure the shim is built before the C extension objects.
treedb_tam.o treedb_bgworker.o: $(SHIM_SO)

.PHONY: go-shim
go-shim: $(SHIM_SO)

# Remove the shim when cleaning.
clean: clean-shim
clean-shim:
	rm -f $(SHIM_SO)
