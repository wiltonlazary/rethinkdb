# Windows build

ifeq (1,$(DEBUG))
  CONFIGURATION := Debug
else
  CONFIGURATION := Release
endif

$/RethinkDB.vcxproj.xml:
	test -e $@ || cp $/mk/RethinkDB.vcxproj.xml $@

$/RethinkDB.vcxproj: $/RethinkDB.vcxproj.xml $/mk/RethinkDB.vcxproj.xml
	$P GEN
	cscript /nologo $/mk/gen-vs-project.js

.PHONY: windows-all
windows-all: $/build/$(PLATFORM)/$(CONFIGURATION)/RethinkDB.exe

SOURCES := $(shell find $(SOURCE_DIR) -name '*.cc' -not -name '\.*')

ifeq (1,$(DEBUG))
  SOURCES := $(filter-out $(SOURCE_DIR)/unittest/%,$(SOURCES))
endif

LIB_DEPS := $(foreach dep, $(FETCH_LIST), $(SUPPORT_BUILD_DIR)/$(dep)_$($(dep)_VERSION)/$(INSTALL_WITNESS))

MSBUILD_FLAGS := /nologo /maxcpucount
MSBUILD_FLAGS += /p:Configuration=$(CONFIGURATION)
MSBUILD_FLAGS += /p:Platform=$(PLATFORM)
MSBUILD_FLAGS += /p:PlatformToolset=v140 # TODO ATN: this should not be necessary
MSBUILD_FLAGS += $(if $(ALWAYS_MAKE),/t:rebuild)
ifneq (1,$(VERBOSE))
  MSBUILD_FLAGS += /verbosity:minimal
endif

$/build/$(PLATFORM)/$(CONFIGURATION)/RethinkDB.exe: $/RethinkDB.vcxproj $(SOURCES) $(LIB_DEPS)
	$P MSBUILD
	"$(MSBUILD)" $(MSBUILD_FLAGS) $<

.PHONY: build-clean
build-clean:
	$P RM $(BUILD_ROOT_DIR)
	rm -rf $(BUILD_ROOT_DIR)
