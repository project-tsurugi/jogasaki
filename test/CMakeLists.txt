file(GLOB SRCS
        "*.cpp"
        "executor/*.cpp"
        "executor/shuffle/*.cpp"
        "scheduler/*.cpp"
        "mock/*.cpp"
        )

register_tests(
        TARGET dag_controller
        DEPENDS
            PRIVATE tbb
            PRIVATE takatori
        SOURCES ${SRCS}
)
