#define _GNU_SOURCE
#include <sched.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include "memlib.h"
#include "mm_thread.h"
#include "malloc.h"

/*********** Global Varibales *****************/

int superblk_size = 4096;
size_t block_sizes[9] = {8, 16, 32, 64, 128, 256, 512, 1024, 2048};
size_t page_size;

/************** Structures **********************/

typedef struct superblk {
	int type;
	size_t block_class;
	char block_bit_map[64]; // bit-map representing when block are used
	int u;	// num of blocks currently used
	int heap_num; // num of its heap owner
	struct superblk *prev;
	struct superblk *next;
} superblk_t;

typedef struct node {
	// used for large mallocs
	int type;
	int npages;
	int heap_num;
	void *prev;
	void *next;

} node_t;

typedef struct heap {

	pthread_mutex_t heap_lock;
	int a;	// allocated bytes for the heap
	int u;	// used bytes within the heap
	node_t *largeblks;
	// array of superblock bins from each of the 9 defined size classes and 6 seperate arrays for each class to determine how empty they are (empty, 1-24%, 25-49%, 50-74%, 75-99%, full).
	superblk_t *super_bases[9][6];

} heap_t;

heap_t **heaptable;

// global lock
pthread_mutex_t system_lock = PTHREAD_MUTEX_INITIALIZER;

/******** Routines ************/
void *alloc_large(size_t sz, int cpu_id) {
	int num_pgs = ceil((float)(sz + sizeof(node_t))/(float)superblk_size);

	node_t *res = NULL;
	
	// search global heap
	pthread_mutex_lock(&heaptable[0]->heap_lock);
	node_t *curr = heaptable[0]->largeblks;

	while(curr) {
		if (curr->npages >= num_pgs) {
			
			res = curr;
			node_t *res_next = NULL;
			node_t *res_prev = NULL;

			if (curr->npages > num_pgs) {
				node_t *head = (node_t *)((unsigned long)curr + (num_pgs * page_size));
				
				head->type = 1;
				head->npages = curr->npages - num_pgs;
				head->heap_num = 0;
				head->prev = curr->prev;
				head->next = curr->next;

				res_next = head;
				res_prev = head;
			}
			else {
				res_next = curr->next;
				res_prev = curr->prev;
			}

			if (curr->prev) {
				((node_t *)curr->prev)->next = res_next;
			} else {
				heaptable[0]->largeblks = res_next;
			}

			if (curr->next) {
				((node_t *)curr->next)->prev = res_prev;	
			}

			break;
		}

		curr = curr->next;
	}

	pthread_mutex_unlock(&heaptable[0]->heap_lock);

	if(!res) {
		// use sbrk to get more memory from system
		pthread_mutex_lock(&system_lock);
		res = (node_t *)mem_sbrk(num_pgs * page_size);
		pthread_mutex_unlock(&system_lock);
	}

	// Insert new block into heap's large block array
	pthread_mutex_lock(&heaptable[cpu_id+1]->heap_lock);

	if (heaptable[cpu_id+1]->largeblks) {
		heaptable[cpu_id+1]->largeblks->prev = res;
	}

	res->next = heaptable[cpu_id+1]->largeblks;
	res->prev = NULL;
	heaptable[cpu_id+1]->largeblks = res;

	res->type = 1;
	res->npages = num_pgs;
	res->heap_num = cpu_id;
	pthread_mutex_unlock(&heaptable[cpu_id+1]->heap_lock);

	res = res + 1;

	return res;
}

//Free large allocated blocks
void dealloc_large(node_t *blk) {
	
	int heap_num = blk->heap_num;

	//remove block from processor heap
	pthread_mutex_lock(&heaptable[heap_num]->heap_lock);
	
	if (blk->prev) {
		((node_t *)blk->prev)->next = blk->next;
	} else {
		heaptable[heap_num]->largeblks = blk->next;
	}

	if (blk->next) {
		((node_t *)blk->next)->prev = blk->prev;
	}
	
	pthread_mutex_unlock(&heaptable[heap_num]->heap_lock);

	// insert into global heap
	pthread_mutex_lock(&heaptable[0]->heap_lock);

	if (heaptable[0]->largeblks) {
		heaptable[0]->largeblks->prev = blk;
	}

	blk->next = heaptable[0]->largeblks;
	blk->prev = NULL;
	heaptable[0]->largeblks = blk;

	pthread_mutex_unlock(&heaptable[0]->heap_lock);

	return;
}

// Creates a new superblock to be placed in the global heap located
// at heap num 0 and returns the pointer to the superblock
superblk_t *create_superblk(void *ptr, size_t blocksize) {
	
	superblk_t *super = (superblk_t *)ptr;

	super->type = 0;
	super->block_class = blocksize;
	memset(&super->block_bit_map, 0, 64);
	super->u = 0;

	// first blocks are reserved for metadata, for small allocs
	if (blocksize < sizeof(superblk_t) * 2) {
		int blks_used = ceil((float)sizeof(superblk_t) / (float)blocksize);
		super->u = blks_used;

		if (blks_used <= 8) {
			super->block_bit_map[0] = (char)pow(2, blks_used) - 1;
		
		}else {
			super->block_bit_map[0] = 255;
			super->block_bit_map[1] = (char)pow(2, blks_used % 8) - 1;	
		}

		heaptable[0]->u += blks_used * super->block_class;
	}

	super->heap_num = 0;
	super->prev = NULL;
	super->next = NULL;

	return super;
	

}

void move_superblk(superblk_t *super, int *src, int *dest) {

	int src_hp_num = src[0];
	int src_class = src[1];
	int src_full = src[2];

	int dest_hp_num = dest[0];
	int dest_class = dest[1];
	int dest_full = dest[2];

	

	// remove super from src
	if (super->next) {
		super->next->prev = super->prev;
	}
	
	if (super->prev) {
		super->prev->next = super->next;
	}

	else {
		heaptable[src_hp_num]->super_bases[src_class][src_full] = super->next;
		
	}

	// No insert it to the destination heap

	if (heaptable[dest_hp_num]->super_bases[dest_class][dest_full]) {
		
		heaptable[dest_hp_num]->super_bases[dest_class][dest_full]->prev = super;

	}

	super->next = heaptable[dest_hp_num]->super_bases[dest_class][dest_full];
	super->prev = NULL;

	heaptable[dest_hp_num]->super_bases[dest_class][dest_full] = super;

	if (dest_hp_num != src_hp_num) {
		super->heap_num = dest_hp_num;

		int total_bytes = super->u * super->block_class;
		heaptable[src_hp_num]->u -= total_bytes;
		heaptable[src_hp_num]->a -= superblk_size;
		heaptable[dest_hp_num]->u += total_bytes;
		heaptable[dest_hp_num]->a += superblk_size;

	}

	
}

// looks for free block given the fullness bit-map of a superblock
int find_blk(char *blk_map, size_t blocksize) {
	float num = (float)superblk_size / (float)(blocksize * 8);
	int k;
	
	// if num is less than size of char
	if (num < 1) {
		
		for (k=0; k < round(8 * num); k++) {
			if (!blk_map[0] % (int)pow(2, k)) {
				return k;
			}
		}
	}
	
	int i;

	for (i=0; i < num; i++) {
		// if less than there must be an available block
		if (blk_map[i] < 255) {
			for (k=0; k<8; k++) {
				if (!((blk_map[i] >> k) % 2)) {
					return i * 8 + k;
				}
			}
		}
	}

	// if no free block was found
	return -1;
}

int get_cpu_id() {
	int thread_id = getTID();
	int cpu_id = -1;

	cpu_set_t mask;
	CPU_ZERO(&mask);

	int num_processors = getNumProcessors();

	if (sched_getaffinity(thread_id, sizeof(cpu_set_t), &mask) == 0) {
		int i;
		for (i=0; i<num_processors; i++) {
			if (CPU_ISSET(i, &mask)) {
				cpu_id = i;
				break;
			}
		}
	} else {
		perror("sched_getaffinity failed");
	}

	if (cpu_id < 0) {
		perror("unable to recieve cpu ID");
	}

	return cpu_id;
}

void *mm_malloc(size_t sz)
{

	int cpu_id = get_cpu_id();

	if (sz > superblk_size / 2) {
		return alloc_large(sz,cpu_id);
	}

	int i;
	int f;
	int sz_id;
	
	superblk_t *super_res = NULL;
	char use = 0;
	size_t block_class = 8;
	
	// get size of blocks
	for (sz_id=0; sz_id<9; sz_id++) {
		if (sz <= block_sizes[sz_id]) {
			block_class = block_sizes[sz_id];
			break;
		}
	}

	pthread_mutex_lock(&heaptable[cpu_id+1]->heap_lock);

	// now check the fullness bins for most full block to most empty
	for (i=4; i>=0; i--) {
		superblk_t *super = heaptable[cpu_id+1]->super_bases[sz_id][i];
		while(super) {
			int x_use = 0;
			if (!(super->block_bit_map[0] % 2)) {
				if (sz > block_class - sizeof(superblk_t)) {
					x_use = 1;
				} else {
					use = 1;
					super_res = super;
					f = i;
					break;
				}
			}

			if (super->u + x_use < superblk_size/block_class) {
				super_res = super;
				f = i;
				break;
			}

			super = super->next;
		}

		if (super_res) {
			break;
		}
	}

	// if no reusable block in it's own heap check global heap and
	// if need be allocate more memory using sbrk

	if (!super_res) {
		int src[3] = {0, 0, 0};
		pthread_mutex_lock(&heaptable[0]->heap_lock);
		
		// check global heap empty bins
		for (i=0; i<9; i++) {
			if (heaptable[0]->super_bases[i][0]) {
				super_res = heaptable[0]->super_bases[i][0];
				src[0] = 0;
				src[1] = i;
				src[2] = 0;

				superblk_t *next_res = super_res->next;
				heaptable[0]->u -= super_res->u * super_res->block_class;
				super_res = create_superblk(super_res, block_class);
				super_res->next = next_res;
				break;
			}
		}
		// check the next bin
		if (!super_res && heaptable[0]->super_bases[sz_id][1]) {
			super_res = heaptable[0]->super_bases[sz_id][1];
			src[0] = 0;
			src[1] = sz_id;
			src[2] = 1;
		}

		if (!super_res) {
			pthread_mutex_lock(&system_lock);
			void *ptr = mem_sbrk(page_size);
			pthread_mutex_unlock(&system_lock);

			if (!ptr) {
				return NULL;
			}

			// init super block
			super_res = create_superblk(ptr, block_class);

			heaptable[0]->super_bases[sz_id][0] = super_res;
			heaptable[0]->a += superblk_size;
			
			src[0] = 0;
			src[1] = sz_id;
			src[2] = 0;

		}

		// move superblock to heap's super_bases array
		int dest[3] = {cpu_id+1,sz_id,1};
		move_superblk(super_res, src, dest);
		f = 1;
		pthread_mutex_unlock(&heaptable[0]->heap_lock);
	}

	// find a free block
	void *blk;
	if (use) {
		blk = (char *)super_res + sizeof(superblk_t);
		super_res->block_bit_map[0]++;
	}
	else {
		// check bit maps for free blocks
		int blk_id = find_blk(super_res->block_bit_map, block_class);
		if (blk_id == -1) {
			return NULL;
		}

		blk = (char *)super_res + blk_id * block_class;

		super_res->block_bit_map[blk_id/8] += pow(2, blk_id % 8);
	}

	super_res->u++;
	heaptable[cpu_id+1]->u += block_class;

	// if bin has changed fullness, move it again

	float new_per = (float)super_res->u / (float)(superblk_size / block_class);
	int new_f = (int)(new_per * 4) + 1;
	
	if (new_f != f) {
		int src[3] = {cpu_id+1,sz_id,f};
		int dest[3] = {cpu_id+1,sz_id,new_f};
		move_superblk(super_res, src, dest);
	}

	pthread_mutex_unlock(&heaptable[cpu_id+1]->heap_lock);

	return blk;
}

void mm_free(void *ptr)
{
	int sz_id;
	int heap_num;

	// move up to read header data
	
	void *pg = (int *)((unsigned long)ptr - ((unsigned long)ptr % mem_pagesize()));

	unsigned long offset = (unsigned long)ptr % mem_pagesize();
	int type = *(int *)pg;

	if (type) {
		// large alloc
		dealloc_large((node_t *)pg);
		return;	
	}

	// free block from superblock
	superblk_t *super = (superblk_t *)pg;
	heap_num = super->heap_num;

	pthread_mutex_lock(&heaptable[heap_num]->heap_lock);

	int blk_id = offset / super->block_class;

	super->block_bit_map[blk_id/8] -= pow(2, blk_id % 8);

	float per = (float)super->u / (float)(superblk_size / super->block_class);
	int f = (int)(per * 4) + 1;

	for (sz_id=0;sz_id<9;sz_id++) {
		if (super->block_class <= block_sizes[sz_id]) {
			break;
		}
	}
	
	super->u--;
	heaptable[heap_num]->u -= super->block_class;

	// Update bins

	float new_per = (float)super->u / (float)(superblk_size / super->block_class);
	int new_f = (int)(new_per * 4) + 1;

	if (new_f != f) {
		int src[3] = {heap_num, sz_id, f};
		int dest[3] = {heap_num, sz_id, new_f};
		move_superblk(super, src, dest);
	}

	// if global heap
	if (heap_num == 0) {
		pthread_mutex_unlock(&heaptable[0]->heap_lock);
		return;
	}

	// if heap has passed the emptiness threshold
	// than return to global heap
	if (heaptable[heap_num]->u < heaptable[heap_num]->a / 4 &&
		heaptable[heap_num]->u < heaptable[heap_num]->a - 8 * superblk_size) {

		int i;
		int k;
		int src[3] = {0,0,0};
		superblk_t *super_res = NULL;

		for (k=0;k<=1;k++) {
			for (i=0;i<9;i++) {
				if (heaptable[heap_num]->super_bases[i][k]) {
					super_res = heaptable[heap_num]->super_bases[i][k];
					src[0] = heap_num;
					src[1] = i;
					src[2] = k;
					break;

				}
			}

			if (super_res) {
				break;
			}
		}

		// move super to global heap
		int dest[3] = {0, src[1], src[2]};
		move_superblk(super_res, src, dest);
	}

	pthread_mutex_unlock(&heaptable[heap_num]->heap_lock);

}


int mm_init(void)
{
	if (!mem_init()) {
		int ncpu = getNumProcessors();

		page_size = mem_pagesize();
		heaptable = (heap_t **) mem_sbrk(page_size);

		int i;

		for (i=0; i<=ncpu; i++) {
			// allocate page for each heap
			heaptable[i] = (heap_t *)mem_sbrk(page_size);

			// init heaps

			pthread_mutex_init(&heaptable[i]->heap_lock, NULL);
			heaptable[i]->a = 0;
			heaptable[i]->u = 0;
			heaptable[i]->largeblks = NULL;
			memset(heaptable[i]->super_bases, 0, sizeof(superblk_t *) * 9 * 6);
		}
	}

	return 0;
}
