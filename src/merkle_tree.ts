import { LevelUp, LevelUpChain } from 'levelup';
import subleveldown from 'subleveldown';
import { HashPath } from './hash_path';
import { Sha256Hasher } from './sha256_hasher';
import { Hasher } from './hasher';

const MAX_DEPTH = 32;
const LEAF_BYTES = 64; // All leaf values are 64 bytes.

class LazyTree {
  private hasher: Hasher;
  private nodes: Map<number, Buffer> = new Map();
  private depth: number;
  private emptyHashes: { [key: number]: Buffer } = {};

  constructor(hasher: Hasher, depth: number) {
    this.hasher = hasher;
    this.depth = depth;
    this.populateEmptyHashes();
  }

  private populateEmptyHashes() {
    var hash = this.hasher.hash(Buffer.alloc(LEAF_BYTES));
    for (let d = 0; d <= this.depth; ++d) {
      this.emptyHashes[this.depth - d] = hash;
      hash = this.hasher.compress(hash, hash);
    }
  }

  /**
   * Get the root of the tree.
   * @returns root of the tree
   */
  getRoot() {
    return this.get(0);
  }

  /**
   * Get the node at the given index.
   * @param index
   * @returns node at the index. If the node is not present, return the empty hash at the depth.
   */
  get(index: number) {
    const depth = this.calculateIndexDepth(index);
    return this.nodes.get(index) || this.emptyHashes[depth];
  }

  /**
   * Set the node at the given index.
   * @param index
   * @param value
   */
  set(index: number, hash: Buffer) {
    this.nodes.set(index, hash);

    let parentIdx = index;
    while (parentIdx >= 0) {
      parentIdx = parentIdx % 2 === 0 ? (parentIdx - 2) / 2 : (parentIdx - 1) / 2;
      if (parentIdx < 0) {
        break;
      }

      const left = this.get(parentIdx * 2 + 1);
      const right = this.get(parentIdx * 2 + 2);
      this.nodes.set(parentIdx, this.hasher.compress(left, right));
    }
  }

  /**
   * Get the hash path for the given index.
   * @param index
   * @returns hash path for the index.
   */
  getPath(index: number) {
    const path: Buffer[][] = [];

    let parentIdx = index;
    while (parentIdx >= 0) {
      parentIdx = parentIdx % 2 === 0 ? (parentIdx - 2) / 2 : (parentIdx - 1) / 2;
      if (parentIdx < 0) {
        break;
      }

      const left = this.get(parentIdx * 2 + 1);
      const right = this.get(parentIdx * 2 + 2);
      path.push([left, right]);
    }

    return path;
  }

  /**
   * Calculate the depth of an index. Assuming indexing starts from
   * the root, the depth would be the closest power of 2 for the (index + 1).
   * @param index
   * @returns depth of the element
   */
  private calculateIndexDepth(index: number) {
    return Math.floor(Math.log2(index + 1));
  }
}

/**
 * The merkle tree, in summary, is a data structure with a number of indexable elements, and the property
 * that it is possible to provide a succinct proof (HashPath) that a given piece of data, exists at a certain index,
 * for a given merkle tree root.
 */
export class MerkleTree {
  private hasher = new Sha256Hasher();
  private elementsDb: LevelUp;
  private tree: LazyTree;

  /**
   * Constructs a new MerkleTree instance, either initializing an empty tree, or restoring pre-existing state values.
   * Use the async static `new` function to construct.
   *
   * @param db Underlying leveldb.
   * @param name Name of the tree, to be used when restoring/persisting state.
   * @param depth The depth of the tree, to be no greater than MAX_DEPTH.
   */
  constructor(private db: LevelUp, private name: string, private depth: number) {
    if (!(depth >= 1 && depth <= MAX_DEPTH)) {
      throw Error('Bad depth');
    }
    this.name = name;
    this.depth = depth;

    this.tree = new LazyTree(this.hasher, depth);

    this.db = db;
    this.elementsDb = subleveldown(db, name);
  }

  /**
   * Constructs or restores a new MerkleTree instance with the given `name` and `depth`.
   * The `db` contains the tree data.
   */
  static async new(db: LevelUp, name: string, depth = MAX_DEPTH) {
    const meta: Buffer = await db.get(Buffer.from(name)).catch(() => {});
    if (meta) {
      const root = meta.slice(0, 32);
      const depth = meta.readUInt32LE(32);
      const tree = new MerkleTree(db, name, depth);
      await tree.restoreElements();
      if (!tree.getRoot().equals(root)) {
        throw Error('Root mismatch');
      }
      return tree;
    } else {
      const tree = new MerkleTree(db, name, depth);
      await tree.writeMetaData();
      return tree;
    }
  }

  private async writeMetaData(batch?: LevelUpChain<string, Buffer>) {
    const data = Buffer.alloc(40);
    this.getRoot().copy(data);
    data.writeUInt32LE(this.depth, 32);
    if (batch) {
      batch.put(this.name, data);
    } else {
      await this.db.put(this.name, data);
    }
  }

  private async restoreElements() {
    return new Promise((resolve, reject) => {
      const stream = this.elementsDb.createReadStream();
      stream.on('data', (data) => {
        const index = parseInt(data.key.toString());
        this.tree.set(this.elementTreeIndex(index), Buffer.from(data.value, 'hex'));
      });
      stream.on('end', resolve);
      stream.on('error', reject);
    });
  }

  getRoot() {
    return this.tree.getRoot();
  }

  /**
   * Returns the hash path for `index`.
   * e.g. To return the HashPath for index 2, return the nodes marked `*` at each layer.
   *     d3:                                            [ root ]
   *     d2:                      [*]                                               [*]
   *     d1:         [*]                      [*]                       [ ]                     [ ]
   *     d0:   [ ]         [ ]          [*]         [*]           [ ]         [ ]          [ ]        [ ]
   */
  async getHashPath(index: number) {
    return new HashPath(this.tree.getPath(this.elementTreeIndex(index)));
  }

  /**
   * Updates the tree with `value` at `index`. Returns the new tree root.
   */
  async updateElement(index: number, value: Buffer) {
    const hash = this.hasher.hash(value);
    this.tree.set(this.elementTreeIndex(index), hash);

    await this.elementsDb.put(index.toString(), hash.toString('hex'));
    await this.writeMetaData();

    return this.getRoot();
  }

  /**
   * Calculates the 'true' index of the leaf node in the tree.
   * @param index
   * @returns the index of the leaf node in the tree.
   */
  private elementTreeIndex(index: number) {
    return index + Math.pow(2, this.depth) - 1;
  }
}
