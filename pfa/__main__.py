import argparse
from importlib import import_module

# from pfa.dash.app import app


def main():
    parser = argparse.ArgumentParser(description="Portfolio analytics tool")
    parser.add_argument("--workflow", "-wf", metavar="wf", type=str)
    parser.add_argument("--dashboard", "-d", action="store_true")
    args = parser.parse_args()

    if args.workflow:
        workflow = import_module(f"pfa.workflows.{args.workflow}")
        workflow.flow.run()

    # if args.dashboard:
    #     app.run_server(debug=True)


if __name__ == "__main__":
    main()
