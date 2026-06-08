PG_CONFIG   ?= pg_config
PGXS        := $(shell $(PG_CONFIG) --pgxs)
PKGLIBDIR   := $(shell $(PG_CONFIG) --pkglibdir)

# Go shared library
SHIM_DIR    := go
SHIM_SO     := $(PKGLIBDIR)/treedb_shim.so

# Pass the shim path as a compile-time constant so the background worker
# knows where to dlopen it.
PG_CPPFLAGS := -I$(CURDIR) -DTDB_SHIM_PATH=\"$(SHIM_SO)\"

MODULE_big  := treedb_pgext
OBJS        := treedb_tam.o treedb_bgworker.o
EXTENSION   := treedb_pgext
DATA        := treedb_pgext--1.0.sql

include $(PGXS)

# Build the Go shared library before the C objects.
# The Go toolchain generates treedb_shim.h automatically; we don't use it
# (we load via dlopen at runtime), but we write it to /dev/null to keep the
# build directory clean.
$(SHIM_SO): $(SHIM_DIR)/treedb_shim.go $(SHIM_DIR)/go.mod
	@echo "==> Building Go shim..."
	cd $(SHIM_DIR) && \
	  CGO_ENABLED=1 go build \
	    -buildmode=c-shared \
	    -o $(SHIM_SO) \
	    .
	@# The auto-generated header goes into /dev/null; we use dlopen, not static linking.
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
