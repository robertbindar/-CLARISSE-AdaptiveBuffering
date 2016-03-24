#ifndef DLLIST_INCLUDE
#define DLLIST_INCLUDE


#include <stdlib.h>

typedef struct dllist_link {
  struct dllist_link *prev, *next;
} dllist_link;

typedef struct dllist {
  struct dllist_link *head, *tail;
} dllist;

/* this is the basic idea of this double-linked-list:
 * 
 * dllist root;
 * dllist_link elementX;
 * dllist_link elementY;
 * dllist_link elementZ;
 *
 *                     [   root   ]
 *      ,--------------[head)(tail]-------------,
 *      |                                       |
 *      V                                       V
 * [ elementX ] <------[prev)(next]------> [ elementZ ]
 * [prev)(next]------> [ elementY ] <------[prev)(next]
 *   |                                             |
 *   V                                             V
 *  NULL                                         NULL
 *
 * This include file provide only the basic functions for
 * inserting and removing elements. The idea is to keep the
 * "library" small.
 * Traversing etc. can be done by directly accessing the
 * members.
 */


/* This is used to initialize the root pointers:
 *   dllist list = DLLIST_INITIALIZER; */
#define DLLIST_INITIALIZER {head:NULL, tail:NULL}

/* get the struct for this entry
 * @ptr:	the &dllist_link pointer.
 * @type:	the type of the struct this is embedded in.
 * @member:	the name of the dllist within the struct. */
#define DLLIST_ELEMENT(ptr, type, member) \
	((type *)((char *)(ptr)-(unsigned long)(&((type *)0)->member)))

/* iah = insert at head
 * insert element at the head */
static inline void dllist_iah(dllist *root, dllist_link *element) {
  element->next = root->head;
  element->prev = NULL;
  if (root->head != NULL)
    root->head->prev = element;
  root->head = element;
  if (root->tail == NULL)
    root->tail = element;
}

/* iat = insert at tail
 * insert element at the tail */
static inline void dllist_iat(dllist *root, dllist_link *element) {
  element->prev = root->tail;
  element->next = NULL;
  if (root->tail != NULL)
    root->tail->next = element;
  root->tail = element;
  if (root->head == NULL)
    root->head = element;
}

/* ibefore = insert before 
 * insert element before another element "here" */
static inline void dllist_ibefore(dllist *root, dllist_link *here,
                                  dllist_link *element) {
  if ((root->head == root->tail) || (root->head == here)) {
    dllist_iah(root, element);
  } else {
    element->prev = here->prev;
    element->next = here;
    here->prev->next = element;
    here->prev = element;
  }
}

/* iafter = insert after 
 * insert element after another element "here" */
static inline void dllist_iafter(dllist *root, dllist_link *here,
                                  dllist_link *element) {
  if ((root->head == root->tail) || (root->tail == here)) {
    dllist_iat(root, element);
  } else {
    element->next = here->next;
    element->prev = here;
    here->next->prev = element;
    here->next = element;
  }
}




/* rem = remove element from list
 * element should not be NULL ! */
static inline void dllist_rem(dllist *root, dllist_link *element) {
  if (root->head == element)
    root->head = element->next;
  if (root->tail == element)
    root->tail = element->prev;
  if (element->next != NULL)
    element->next->prev = element->prev;
  if (element->prev != NULL)
    element->prev->next = element->next;
  element->prev = element->next = NULL;
}

/* remove head element and return it */
static inline dllist_link *dllist_rem_head(dllist *root) {
  dllist_link *head = root->head;
  if (head) dllist_rem(root, head);
  return head;
}

/* remove element at the tail and return it */
static inline dllist_link *dllist_rem_tail(dllist *root) {
  dllist_link *tail = root->tail;
  if (tail)
    dllist_rem(root, tail);
  return tail;
}

/* return tail */
static inline dllist_link *dllist_tail(dllist *root) {
  return root->tail;
}

/* return head */
static inline dllist_link *dllist_head(dllist *root) {
  return root->head;
}

static inline void dllist_init(dllist *root) {
  root->head = root->tail = NULL;
}

static inline int dllist_is_empty(dllist *root) {
  return (root->head == NULL);
}

static inline void dllist_move_to_head(dllist *root, dllist_link *element) {
  if (element != root->head) {
    dllist_rem(root, element);
    dllist_iah(root, element);
  }
}

/* simple linked list */
/*
#ifndef NULL
#define NULL (void *)0
#endif


typedef struct item {
  struct item *next;
  void* data;
} item,*item_p;

#define DATA(i_p) (i_p->data)
#define NEXT(i_p) (i_p->next)
#define NEXT_CIRCULAR(it,l_p) ((it->next)?(it->next):((item_p)HEAD(l_p)))
#define PEEK(l_p) ((l_p)->head->data)
#define PEEK_AT_END(l_p) ((l_p)->tail->data)

typedef struct List{
  item_p head, tail;
  int length;
} List,*List_p;


#define TAIL(l_p) ((l_p)->tail)
#define HEAD(l_p) ((l_p)->head)
#define LIST_LENGTH(l_p) (l_p->length)
#define HAS_ONE_ELEMENT(l_p) ((HEAD(l_p)==TAIL(l_p))&& \
			      (HEAD(l_p)!=NULL))


void init_list(List_p);
List_p alloc_init_list(void);
int empty_list(List_p);
int add_in_front(void *,List_p);
void * dequeue (List_p);
void * peek (List_p);
int enqueue(void *,List_p);
item_p enqueue2(void *,List_p);
List_p list_append(List_p,List_p);
void * remove_item(item_p,List_p);
int remove_data(void* data, List_p l);
void free_list_content(List_p);
void free_list_content2(List_p);
void free_list(List_p);
void free_list2(List_p); //when the pointer is used as int 
void dump_int_list(List_p l);
*/

#endif
