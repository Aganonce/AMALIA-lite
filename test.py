import amalia
import sys

if __name__ == '__main__':
    cfg_path = 'config/config.yaml'
    if len(sys.argv) > 1:
        cfg_path = sys.argv[1]

    sim = amalia.run(cfg_path)