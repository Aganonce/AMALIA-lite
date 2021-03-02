import amalia
import sys

if __name__ == '__main__':
    cfg_path = 'config/compare.yaml'
    if len(sys.argv) > 1:
        cfg_path = sys.argv[1]

    sim = amalia.compare(cfg_path)