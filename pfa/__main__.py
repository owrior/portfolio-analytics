import argparse
from importlib import import_module


def main():
    parser = argparse.ArgumentParser(description="Portfolio analytics tool")
    parser.add_argument("--workflow", "-wf", metavar="wf", type=str)
    args = parser.parse_args()
    workflow = import_module(f"pfa.workflows.{args.workflow}")
    workflow.flow.run()


if __name__ == "__main__":
    main()
