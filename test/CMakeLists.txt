file(GLOB SRCS
        "*.cpp"
        "executor/*.cpp"
        "executor/shuffle/*.cpp"
        "executor/process/*.cpp"
        "scheduler/*.cpp"
        "mock/*.cpp"
        )

register_tests(
        TARGET jogasaki
        DEPENDS
            PRIVATE tbb
            PRIVATE takatori
        SOURCES ${SRCS}
)
