import {
  Query,
  Args,
  ResolveField,
  Resolver,
  Parent,
  ID,
} from '@nestjs/graphql';
import { Post } from './posts.interfaces';
import { PostsService } from './posts.service';

@Resolver()
export class PostsResolver {
  constructor(private readonly postsService: PostsService) {}

  @Query()
  findPost(@Args({ name: 'id', type: () => ID }) id: number) {
    return this.postsService.findOne(id);
  }

  @Query()
  getPosts() {
    return this.postsService.all();
  }

  @ResolveField('user')
  user(@Parent() post: Post): any {
    return { __typename: 'User', id: post.authorId };
  }
}
